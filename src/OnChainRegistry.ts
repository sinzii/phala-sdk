import { type ApiPromise, Keyring } from '@polkadot/api'
import { type KeyringPair } from '@polkadot/keyring/types'
import type { Result, U64 } from '@polkadot/types'
import { AccountId } from '@polkadot/types/interfaces'
import { HexString } from '@polkadot/util/types'
import { cryptoWaitReady } from '@polkadot/util-crypto'
import { AccountId32, AccountId32Like } from 'dedot/codecs'
import systemAbi from './abis/system.json'
import { PhalaTypesContractClusterInfo } from './chaintypes/phala'
import { PinkContractPromise } from './contracts/PinkContract'
import { PinkLoggerContractPromise } from './contracts/PinkLoggerContract'
import { ackFirst } from './ha/ack-first'
import { type CertificateData, signCertificate } from './pruntime/certificate'
import createPruntimeClient from './pruntime/createPruntimeClient'
import { pruntime_rpc } from './pruntime/proto'
import { PhalaClient } from './types'

// @FIXME: We not yet cover `as` and the `OnlyOwner` scenario.
export interface CreateOptions {
  autoConnect?: boolean
  clusterId?: string
  workerId?: string
  pruntimeURL?: string
  systemContractId?: string
  skipCheck?: boolean
  strategy?:
    | 'ack-first'
    | ((
        api: PhalaClient,
        clusterId: string
      ) => Promise<Readonly<[string, string, ReturnType<typeof createPruntimeClient>]>>)
}

export interface WorkerInfo {
  pubkey: string
  clusterId: string
  endpoints: {
    default: string
    v1?: string[]
  }
}

export interface PartialWorkerInfo {
  clusterId?: string
  pruntimeURL: string
}

export interface StrategicWorkerInfo {
  clusterId?: string
  strategy:
    | 'ack-first'
    | ((api: ApiPromise, clusterId: string) => Promise<Readonly<[string, ReturnType<typeof createPruntimeClient>]>>)
}

export class OnChainRegistry {
  public api: PhalaClient

  public clusterId: string | undefined
  public clusterInfo: PhalaTypesContractClusterInfo | undefined

  public workerInfo: WorkerInfo | undefined

  #ready: boolean = false
  #phactory: pruntime_rpc.PhactoryAPI | undefined

  #alice: KeyringPair | undefined
  #cert: CertificateData | undefined

  #systemContract: PinkContractPromise | undefined
  #loggerContract: PinkLoggerContractPromise | undefined

  constructor(api: PhalaClient) {
    this.api = api
  }

  public async getContractKey(contractId: AccountId32Like) {
    const contractAccountId = new AccountId32(contractId)
    const contractKey = await this.api.query.phalaRegistry.contractKeys(contractAccountId.raw)
    if (!contractKey) {
      return undefined
    }
    return contractKey.toString()
  }

  public async getContractKeyOrFail(contractId: string) {
    const contractKey = await this.getContractKey(contractId)
    if (!contractKey) {
      throw new Error(`Contract ${contractId} not found in cluster.`)
    }
    return contractKey
  }

  public isReady() {
    return this.#ready
  }

  get phactory() {
    if (!this.#ready || !this.#phactory) {
      throw new Error('You need initialize OnChainRegistry first.')
    }
    return this.#phactory
  }

  get gasPrice() {
    if (!this.#ready || !this.clusterInfo || !this.clusterInfo.gasPrice) {
      throw new Error('You need initialize OnChainRegistry first.')
    }
    return this.clusterInfo.gasPrice
  }

  /**
   * Static factory method returns a ready to use PhatRegistry object.
   */
  static async create(api: PhalaClient, options?: CreateOptions) {
    options = { autoConnect: true, ...(options || {}) }
    const instance = new OnChainRegistry(api)
    // We should ensure the wasm & api has been initialized here.
    await Promise.all([cryptoWaitReady(), api.connect()])
    if (options.autoConnect) {
      if (!options.clusterId && !options.workerId && !options.pruntimeURL) {
        await instance.connect()
      }
      // If user specified the pruntimeURL, it should use it first.
      else if (options.pruntimeURL) {
        const workerInfo: PartialWorkerInfo = {
          clusterId: options.clusterId,
          pruntimeURL: options.pruntimeURL,
        }
        await instance.connect(workerInfo)
      } else if (options.clusterId && !options.strategy) {
        await instance.connect({
          clusterId: options.clusterId,
          strategy: 'ack-first',
        } as StrategicWorkerInfo)
      } else if (options.strategy) {
        await instance.connect({
          clusterId: options.clusterId,
          strategy: options.strategy,
        } as StrategicWorkerInfo)
      }
      // Failed back to backward compatible mode.
      else {
        console.warn('Failed back to legacy connection mode, please use pruntimeURL instead.')
        await instance.connect(
          options.clusterId,
          options.workerId,
          options.pruntimeURL,
          options.systemContractId,
          !!options.skipCheck
        )
      }
    }
    return instance
  }

  public async getAllClusters() {
    return await this.api.query.phalaPhatContracts.clusters.pagedEntries()
  }

  public async getClusterInfoById(clusterId: HexString | string) {
    return await this.api.query.phalaPhatContracts.clusters(clusterId as HexString)
  }

  public async getClusters(clusterId?: HexString | string) {
    if (clusterId) {
      return await this.api.query.phalaPhatContracts.clusters(clusterId as HexString)
    } else {
      return await this.getAllClusters()
    }
  }

  public async getEndpoints(workerId?: HexString) {
    if (workerId) {
      return await this.api.query.phalaRegistry.endpoints(workerId)
    }

    return await this.api.query.phalaRegistry.endpoints.pagedEntries()
  }

  public async getClusterWorkers(clusterId?: string): Promise<WorkerInfo[]> {
    let _clusterId = clusterId || this.clusterId
    if (!_clusterId) {
      const clusters = await this.getAllClusters()
      if (!clusters || clusters.length === 0) {
        throw new Error('You need specified clusterId to list workers inside it.')
      }
      _clusterId = clusters[0][0] as string
    }
    const workerIds = await this.api.query.phalaPhatContracts.clusterWorkers(_clusterId as HexString)
    const infos = await this.api.query.phalaRegistry.endpoints.multi(workerIds)

    return infos
      .map((info, idx) => [workerIds[idx], info] as const)
      .filter(([_, maybeEndpoint]) => !!maybeEndpoint)
      .map(
        ([workerId, maybeEndpoint]) =>
          ({
            pubkey: workerId,
            clusterId: _clusterId!,
            endpoints: {
              default: maybeEndpoint!.value[0],
              v1: maybeEndpoint!.value,
            },
          }) as WorkerInfo
      )
  }

  async preparePruntimeClientOrThrows(endpoint: string) {
    // It might not be a good idea to call getInfo() here, but for now both testnet (POC-5 & closed-beta) not yet
    // upgrade to the latest Phactory API, so we need to call it here to make sure that's compatible.
    try {
      const phactory = createPruntimeClient(endpoint)
      await phactory.getInfo({})
      return phactory
    } catch (err) {
      console.error(err)
      throw new Error(
        'Phactory API not compatible, you might need downgrade your @phala/sdk or connect to an up-to-date endpoint.'
      )
    }
  }

  async prepareSystemOrThrows(clusterInfo: PhalaTypesContractClusterInfo) {
    const systemContractId = clusterInfo.systemContract
    if (systemContractId) {
      const systemContractKey = await this.getContractKey(systemContractId)
      if (systemContractKey) {
        this.#systemContract = new PinkContractPromise(this.api, this, systemAbi, systemContractId, systemContractKey)
        this.#loggerContract = await PinkLoggerContractPromise.create(this.api, this, this.#systemContract)
      } else {
        throw new Error(`System contract not found: ${systemContractId}`)
      }
    }
  }

  /**
   * ClusterId: string | null  - Cluster ID, if empty, will try to use the first cluster found in the chain registry.
   * WorkerId: string | null - Worker ID, if empty, will try to use the first worker found in the cluster.
   * PruntimeURL: string | null - Pruntime URL, if empty, will try to use the pruntime URL of the selected worker.
   * systemContractId: string | AccountId | null - System contract ID, if empty, will try to use the system contract ID of the selected cluster.
   * skipCheck: boolean | undefined - Skip the check of cluster and worker has been registry on chain or not, it's for cluster
   *                      deployment scenario, where the cluster and worker has not been registry on chain yet.
   */
  public async connect(worker?: WorkerInfo | PartialWorkerInfo | StrategicWorkerInfo): Promise<void>
  public async connect(
    clusterId?: string | null,
    workerId?: string | null,
    pruntimeURL?: string | null,
    systemContractId?: string | AccountId,
    skipCheck?: boolean
  ): Promise<void>
  public async connect(...args: any[]): Promise<void> {
    this.#ready = false

    if (args.length === 0 || args.length === 1) {
      // Scenario 1: connect to default worker.
      if (args.length === 0) {
        const clusters = await this.getAllClusters()
        if (!clusters || clusters.length === 0) {
          throw new Error('No cluster found.')
        }
        const [clusterId, clusterInfo] = clusters[0]
        const [workerId, endpoint, phactory] = await ackFirst()(this.api, clusterId)
        this.#phactory = phactory
        this.clusterId = clusterId
        this.clusterInfo = clusterInfo
        this.workerInfo = {
          pubkey: workerId,
          clusterId: clusterId,
          endpoints: {
            default: endpoint,
            v1: [endpoint],
          },
        }
        this.#ready = true
        await this.prepareSystemOrThrows(clusterInfo)
        return
      }

      // Scenario 2: connect to specified worker.
      if (args.length === 1 && args[0] instanceof Object) {
        if (args[0].strategy) {
          let clusterId = args[0].clusterId
          let clusterInfo
          if (!clusterId) {
            const clusters = await this.getAllClusters()
            if (!clusters || clusters.length === 0) {
              throw new Error('No cluster found.')
            }
            ;[clusterId, clusterInfo] = clusters[0]
          } else {
            clusterInfo = await this.getClusterInfoById(clusterId)
            if (!clusterInfo) {
              throw new Error(`Cluster not found: ${clusterId}`)
            }
          }
          if (args[0].strategy === 'ack-first') {
            const [workerId, endpoint, phactory] = await ackFirst()(this.api, clusterId)
            this.#phactory = phactory
            this.clusterId = clusterId
            this.clusterInfo = clusterInfo
            this.workerInfo = {
              pubkey: workerId,
              clusterId: clusterId,
              endpoints: {
                default: endpoint,
                v1: [endpoint],
              },
            }
            this.#ready = true
            await this.prepareSystemOrThrows(clusterInfo)
          } else if (typeof args[0].strategy === 'function') {
            const [workerId, phactory] = await args[0].strategy(this.api, clusterId)
            this.#phactory = phactory
            this.clusterId = clusterId
            this.clusterInfo = clusterInfo
            this.workerInfo = {
              pubkey: workerId,
              clusterId: clusterId,
              endpoints: {
                default: phactory.endpoint,
                v1: [phactory.endpoint],
              },
            }
            this.#ready = true
            await this.prepareSystemOrThrows(clusterInfo)
          } else {
            throw new Error(`Unknown strategy: ${args[0].strategy}`)
          }
        }
        // Minimal connection settings, only PRuntimeURL has been specified.
        // clusterId is optional here since we can find it from `getClusterInfo`
        // API
        else if (args[0].pruntimeURL) {
          const partialInfo = args[0] as PartialWorkerInfo
          const pruntimeURL = partialInfo.pruntimeURL
          let clusterId = partialInfo.clusterId
          if (!pruntimeURL) {
            throw new Error('pruntimeURL is required.')
          }
          // We don't here preparePruntimeClientOrThrows here because we need
          // getting related info from PRtunime, we don't need extra check here.
          const phactory = createPruntimeClient(pruntimeURL)
          if (!clusterId) {
            const clusterInfoQuery = await phactory.getClusterInfo({})
            if (clusterInfoQuery?.info?.id) {
              clusterId = clusterInfoQuery.info.id as string
            } else {
              throw new Error(`getClusterInfo is unavailable, please ensure ${pruntimeURL} is valid PRuntime endpoint.`)
            }
          }
          const clusterInfo = await this.getClusterInfoById(clusterId as HexString)
          if (!clusterInfo) {
            throw new Error(`Cluster not found: ${partialInfo.clusterId}`)
          }
          const nodeInfo = await phactory.getInfo({})
          if (!nodeInfo || !nodeInfo.publicKey) {
            throw new Error(`Get PRuntime Pubkey failed.`)
          }
          this.#phactory = phactory
          this.clusterId = clusterId
          this.clusterInfo = clusterInfo
          this.workerInfo = {
            pubkey: nodeInfo.publicKey,
            clusterId: clusterId,
            endpoints: {
              default: pruntimeURL,
            },
          }
          this.#ready = true
          await this.prepareSystemOrThrows(clusterInfo)
        } else {
          const worker = args[0] as WorkerInfo
          const clusterInfo = await this.getClusterInfoById(worker.clusterId)
          if (!clusterInfo) {
            throw new Error(`Cluster not found: ${worker.clusterId}`)
          }
          this.#phactory = await this.preparePruntimeClientOrThrows(worker.endpoints.default)
          this.clusterId = worker.clusterId
          this.clusterInfo = clusterInfo
          this.workerInfo = worker
          this.#ready = true
          await this.prepareSystemOrThrows(clusterInfo)
        }
        return
      }
    }

    console.warn('Deprecated: connect to dedicated worker via legacy mode, please migrate to the new API.')

    // legacy support.
    let clusterId = args[0] as string | undefined
    let workerId = args[1] as string | undefined
    let pruntimeURL = args[2] as string | undefined
    let systemContractId = args[3] as string | AccountId | undefined
    const skipCheck = args[4] as boolean | undefined

    let clusterInfo: PhalaTypesContractClusterInfo

    if (clusterId) {
      clusterInfo = (await this.getClusters(clusterId)) as PhalaTypesContractClusterInfo
      if (!clusterInfo) {
        throw new Error(`Cluster not found: ${clusterId}`)
      }
    } else {
      const clusters = await this.getClusters()
      if (!clusters || !Array.isArray(clusters)) {
        throw new Error('No cluster found.')
      }
      if (clusters.length === 0) {
        throw new Error('No cluster found.')
      }
      clusterId = clusters[0][0] as string
      clusterInfo = clusters[0][1] as PhalaTypesContractClusterInfo
    }

    if (!skipCheck) {
      const endpoints = await this.getEndpoints()
      if (!Array.isArray(endpoints) || endpoints.length === 0) {
        throw new Error('No worker found.')
      }
      if (!workerId && !pruntimeURL) {
        workerId = endpoints[0][0] as string
        pruntimeURL = endpoints[0][1].value[0]
      } else if (pruntimeURL) {
        const endpoint = endpoints.find(([_, v]) => {
          const url = v.value[0]
          return url === pruntimeURL
        })
        if (endpoint) {
          workerId = endpoint[0] as string
        }
      } else if (workerId) {
        const endpoint = endpoints.find(([id, _]) => id === workerId)
        if (!endpoint) {
          throw new Error(`Worker not found: ${workerId}`)
        }

        pruntimeURL = endpoint[1].value[0]
      }
    }

    this.#phactory = createPruntimeClient(pruntimeURL!)

    // It might not be a good idea to call getInfo() here, but for now both testnet (POC-5 & closed-beta) not yet
    // upgrade to the latest Phactory API, so we need to call it here to make sure that's compatible.
    try {
      await this.#phactory.getInfo({})
    } catch (err) {
      console.error(err)
      throw new Error(
        'Phactory API not compatible, you might need downgrade your @phala/sdk or connect to an up-to-date endpoint.'
      )
    }
    this.clusterId = clusterId!
    this.workerInfo = {
      pubkey: workerId!,
      clusterId: clusterId!,
      endpoints: {
        default: pruntimeURL!,
        v1: [pruntimeURL!],
      },
    }
    this.clusterInfo = clusterInfo

    this.#ready = true

    if (this.clusterInfo && this.clusterInfo.systemContract) {
      systemContractId = this.clusterInfo.systemContract
    }
    if (systemContractId) {
      const systemContractKey = await this.getContractKey(systemContractId)
      if (systemContractKey) {
        this.#systemContract = new PinkContractPromise(this.api, this, systemAbi, systemContractId, systemContractKey)
        this.#loggerContract = await PinkLoggerContractPromise.create(this.api, this, this.#systemContract)
      } else {
        throw new Error(`System contract not found: ${systemContractId}`)
      }
    }
  }

  get systemContract() {
    if (this.#systemContract) {
      return this.#systemContract
    }
    console.warn('System contract not found, you might not connect to a health cluster.')
  }

  get alice() {
    if (!this.#alice) {
      const keyring = new Keyring({ type: 'sr25519' })
      this.#alice = keyring.addFromUri('//Alice')
    }
    return this.#alice
  }

  async getAnonymousCert(): Promise<CertificateData> {
    if (!this.#cert) {
      this.#cert = await signCertificate({ pair: this.alice })
    }
    return this.#cert
  }

  resetAnonymousCert() {
    this.#cert = undefined
  }

  async getClusterBalance(address: string | AccountId, cert?: CertificateData) {
    const system = this.#systemContract
    if (!system) {
      throw new Error('System contract not found, you might not connect to a health cluster.')
    }
    if (!cert) {
      cert = await this.getAnonymousCert()
    }
    const [{ output: totalBalanceOf }, { output: freeBalanceOf }] = await Promise.all([
      system.query['system::totalBalanceOf'](cert.address, { cert }, address),
      system.query['system::freeBalanceOf'](cert.address, { cert }, address),
    ])
    return {
      total: (totalBalanceOf as Result<U64, any>).asOk.toBn(),
      free: (freeBalanceOf as Result<U64, any>).asOk.toBn(),
    }
  }

  transferToCluster(address: AccountId32Like, amount: bigint) {
    return this.api.tx.phalaPhatContracts.transferToCluster(amount, this.clusterId as HexString, address)
  }

  get loggerContract() {
    if (this.#loggerContract) {
      return this.#loggerContract
    }
    console.warn('Logger contract not found, you might not connect to a health cluster.')
  }

  get remotePubkey() {
    return this.workerInfo?.pubkey
  }

  get pruntimeURL() {
    return this.workerInfo?.endpoints.default
  }
}
