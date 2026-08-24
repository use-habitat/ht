export interface OwnedSubprocess {
  readonly exited: Promise<number>
  readonly exitCode: number | null
  kill(signal?: number | NodeJS.Signals): void
}

export const terminateOwnedSubprocess = async (
  child: OwnedSubprocess,
  gracePeriodMs = 1_000
): Promise<void> => {
  if (child.exitCode !== null) return
  child.kill("SIGTERM")
  if (await settlesWithin(child.exited, gracePeriodMs)) return
  child.kill("SIGKILL")
  await child.exited.catch(() => undefined)
}

const settlesWithin = async (
  exited: Promise<number>,
  milliseconds: number
): Promise<boolean> => new Promise((resolve) => {
  const timer = setTimeout(() => resolve(false), milliseconds)
  void exited.then(
    () => {
      clearTimeout(timer)
      resolve(true)
    },
    () => {
      clearTimeout(timer)
      resolve(true)
    }
  )
})
