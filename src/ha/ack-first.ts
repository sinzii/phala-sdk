import type { Enum, Text, Vec } from '@polkadot/types'
import { HexString } from '@polkadot/util/types'
import createPruntimeClient from '../pruntime/createPruntimeClient'
import { PhalaClient } from '../types'

export interface PhalaTypesVersionedWorkerEndpoints extends Enum {
  readonly isV1: boolean
  readonly asV1: Vec<Text>
  readonly type: 'V1'
}

async function ack(
  workerId: string,
  endpoint: string
): Promise<Readonly<[string, string, ReturnType<typeof createPruntimeClient>]>> {
  const client = createPruntimeClient(endpoint)
  const info = await client.getInfo({})
  const actually = `0x${info.ecdhPublicKey || ''}`
  if (actually === workerId) {
    return [workerId, endpoint, client] as const
  }
  throw new Error('On-chain worker ID not match to the worker ECDH PublicKey.')
}

/**
 * This is the most simple strategy which pulling list of workers and check their available
 * or not. Return first one that ack succeed.
 */
export function ackFirst() {
  return async function ackFirst(
    client: PhalaClient,
    clusterId: string
  ): Promise<Readonly<[string, string, ReturnType<typeof createPruntimeClient>]>> {
    const workerIds = await client.query.phalaPhatContracts.clusterWorkers(clusterId as HexString)
    const endpointsQuery = await client.query.phalaRegistry.endpoints.multi(workerIds)

    const pairs = endpointsQuery
      .map((i, idx) => [workerIds[idx], i] as const)
      .filter(([_, maybeEndpoint]) => !!maybeEndpoint)
      .map(([workerId, maybeEndpoint]) => [workerId, maybeEndpoint!.value[0]])
    try {
      return await Promise.any(pairs.map(([workerId, endpoint]) => ack(workerId, endpoint)))
    } catch (_err) {
      throw new Error(`No worker available: ${_err}`)
    }
  }
}
