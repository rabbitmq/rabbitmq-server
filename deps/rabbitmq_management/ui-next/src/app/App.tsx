import { useState } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider } from '@tanstack/react-router'
import { ApiError } from '../api/errors'
import { SessionGate } from './Bootstrap'
import { NotificationProvider } from './notifications'
import { createAppRouter } from './router'
import { useColorScheme } from './theme'

export function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        // Client errors such as 404 or a permission denial will not go away on retry.
        retry: (failureCount, error) => !(error instanceof ApiError && error.status < 500) && failureCount < 2,
        refetchIntervalInBackground: false,
        staleTime: 2_000,
      },
    },
  })
}

export function App() {
  const [queryClient] = useState(createQueryClient)
  const [router] = useState(createAppRouter)
  useColorScheme()
  return (
    <QueryClientProvider client={queryClient}>
      <NotificationProvider>
        <SessionGate>
          <RouterProvider router={router} />
        </SessionGate>
      </NotificationProvider>
    </QueryClientProvider>
  )
}
