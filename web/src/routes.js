import React from 'react'

// Dashboard
const Dashboard = React.lazy(() => import('./views/dashboard/Dashboard'))

// Quasar Views
const Registry = React.lazy(() => import('./views/registry/Registry'))
const Assets = React.lazy(() => import('./views/assets/Assets'))
const Mappings = React.lazy(() => import('./views/mappings/Mappings'))
const DataExplorer = React.lazy(() => import('./views/data-explorer/DataExplorer'))

// Charts (kept for trading data visualization)
const Charts = React.lazy(() => import('./views/plugins/charts/Charts'))

const routes = [
  { path: '/', exact: true, name: 'Home' },
  { path: '/dashboard', name: 'Dashboard', element: Dashboard },
  { path: '/registry', name: 'Registry', element: Registry },
  { path: '/assets', name: 'Assets', element: Assets },
  { path: '/mappings', name: 'Mappings', element: Mappings },
  { path: '/data-explorer', name: 'Data Explorer', element: DataExplorer },
  { path: '/charts', name: 'Charts', element: Charts },
]

export default routes
