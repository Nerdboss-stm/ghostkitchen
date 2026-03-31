import { Routes, Route, Navigate } from 'react-router-dom'
import Nav from './components/Nav'
import PipelineOrchestrator from './components/PipelineOrchestrator'
import SchemaExplorer from './components/SchemaExplorer'
import LiveDashboard from './components/LiveDashboard'
import KitchenMap from './components/KitchenMap'
import DataLineage from './components/DataLineage'

export default function App() {
  return (
    <div style={{ background: '#040912', minHeight: '100dvh' }}>
      <Nav />
      <Routes>
        <Route path="/" element={<PipelineOrchestrator />} />
        <Route path="/schema" element={<SchemaExplorer />} />
        <Route path="/dashboard" element={<LiveDashboard />} />
        <Route path="/map" element={<KitchenMap />} />
        <Route path="/lineage" element={<DataLineage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  )
}
