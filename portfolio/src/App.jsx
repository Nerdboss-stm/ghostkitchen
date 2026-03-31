import Nav from './components/Nav'
import PipelineOrchestrator from './components/PipelineOrchestrator'
import SchemaExplorer from './components/SchemaExplorer'
import LiveDashboard from './components/LiveDashboard'

export default function App() {
  return (
    <div className="min-h-screen" style={{ background: '#07071a' }}>
      <Nav />
      <main>
        <PipelineOrchestrator />
        <div className="section-divider" />
        <SchemaExplorer />
        <LiveDashboard />
      </main>
    </div>
  )
}
