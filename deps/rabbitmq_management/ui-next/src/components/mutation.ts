import { useMutation, useQueryClient, type QueryKey } from '@tanstack/react-query'
import { errorMessage } from '../api/errors'
import { useNotify } from '../app/notifications'

interface ApiMutationOptions<TVars, TResult> {
  mutationFn: (vars: TVars) => Promise<TResult>
  invalidate?: QueryKey[]
  success?: string | ((result: TResult, vars: TVars) => string | undefined)
  /** Awaited before the refetch, so that a screen navigated away from is not refetched. */
  onSuccess?: (result: TResult, vars: TVars) => void | Promise<unknown>
}

export function useApiMutation<TVars = void, TResult = void>(options: ApiMutationOptions<TVars, TResult>) {
  const queryClient = useQueryClient()
  const notify = useNotify()
  return useMutation({
    mutationFn: options.mutationFn,
    onSuccess: async (result, vars) => {
      const message = typeof options.success === 'function' ? options.success(result, vars) : options.success
      if (message) notify('success', message)
      await options.onSuccess?.(result, vars)
      await Promise.all((options.invalidate ?? []).map((queryKey) => queryClient.invalidateQueries({ queryKey })))
    },
    onError: (error) => notify('error', errorMessage(error)),
  })
}

/** The browser's confirmation dialog, which is what the classic UI uses for destructive actions. */
export function confirmAction(message: string): boolean {
  return window.confirm(message)
}
