import { useState, useRef, useEffect, useCallback } from 'react'
import { Play, CheckCircle, AlertCircle, Loader, ChevronRight, RotateCcw, ArrowDown } from 'lucide-react'
import { triggerRun, streamRun } from '../lib/api'

const STAGES = [
  { key: 'GENERATE', label: 'Generate', sub: 'Faker · 500 orders · 8k GPS pings', color: '#00d4ff', icon: '⚡' },
  { key: 'BRONZE', label: 'Bronze', sub: 'Raw ingest → PostgreSQL', color: '#9945ff', icon: '🥉' },
  { key: 'SILVER', label: 'Silver', sub: 'Data Vault 2.0 · Identity Resolution', color: '#00d4ff', icon: '🥈' },
  { key: 'GOLD', label: 'Gold', sub: 'Star Schema · 8 dims · 4 facts', color: '#ffaa00', icon: '🥇' },
  { key: 'QUALITY', label: 'Quality', sub: 'Great Expectations · 35 assertions', color: '#00ff88', icon: '✓' },
]

function StageRow({ stage, stageData, isActive, isDone, index }) {
  const color = stage.color
  return (
    <div className="animate-stage-in" style={{ animationDelay: `${index * 80}ms` }}>
      <div className={`flex items-center gap-3 py-3 px-4 rounded-lg transition-all duration-300 ${
        isActive ? 'bg-[#0d0d24] border border-[#1e1e3f]' : ''
      }`}>
        {/* Status icon */}
        <div className="w-7 h-7 flex-shrink-0 flex items-center justify-center">
          {isDone ? (
            <CheckCircle size={20} style={{ color }} />
          ) : isActive ? (
            <Loader size={20} style={{ color }} className="animate-spin" />
          ) : (
            <div className="w-5 h-5 rounded-full border-2 border-[#1e1e3f]" />
          )}
        </div>
        {/* Label */}
        <div className="flex-1 min-w-0">
          <div className={`text-sm font-semibold transition-colors ${isDone || isActive ? 'text-[#e8e8ff]' : 'text-[#4a4a6a]'}`}>
            {stage.icon} {stage.label}
          </div>
          <div className="text-xs text-[#4a4a6a] truncate">{stage.sub}</div>
        </div>
        {/* Duration */}
        {isDone && stageData?.duration_s && (
          <span className="text-xs font-mono text-[#4a4a6a] flex-shrink-0">
            {stageData.duration_s}s
          </span>
        )}
      </div>
      {/* Connector */}
      {index < STAGES.length - 1 && (
        <div className={`w-0.5 h-4 ml-7 my-0.5 transition-all duration-500 rounded-full ${
          isDone ? 'opacity-100' : 'opacity-20'
        }`} style={{ background: isDone ? color : '#1e1e3f' }} />
      )}
    </div>
  )
}

function MetricCard({ label, value, unit, color = '#00d4ff', delay = 0 }) {
  return (
    <div
      className="gk-card p-4 animate-count-up"
      style={{ animationDelay: `${delay}ms`, borderColor: `${color}30` }}
    >
      <div className="text-xs text-[#4a4a6a] mb-1 font-mono uppercase tracking-wider">{label}</div>
      <div className="text-2xl font-bold" style={{ color }}>
        {typeof value === 'number' ? value.toLocaleString() : value}
        {unit && <span className="text-sm text-[#8888aa] ml-1">{unit}</span>}
      </div>
    </div>
  )
}

function JsonFeed({ samples }) {
  const items = samples ? [
    samples.uber_eats_order,
    samples.doordash_order,
    samples.own_app_order,
    samples.sensor,
    samples.gps_ping,
  ].filter(Boolean) : []

  const lines = items.flatMap((item, i) => {
    const json = JSON.stringify(item, null, 2)
    return json.split('\n').map((line, j) => ({ line, color: i % 2 === 0 ? '#00d4ff' : '#9945ff', key: `${i}-${j}` }))
  })

  const doubled = [...lines, ...lines]

  return (
    <div className="overflow-hidden h-full relative">
      <div className="animate-scroll-up">
        {doubled.map((item, idx) => (
          <div key={idx} className="font-mono text-xs py-0.5 px-1 leading-relaxed" style={{ color: item.color }}>
            {item.line}
          </div>
        ))}
      </div>
      <div className="absolute inset-x-0 bottom-0 h-12 bg-gradient-to-t from-[#05050f] to-transparent pointer-events-none" />
    </div>
  )
}

function SubStageTable({ subStages }) {
  return (
    <div className="space-y-1">
      {subStages?.map((s, i) => (
        <div key={i} className="flex items-center gap-3 text-sm py-1.5 border-b border-[#1e1e3f] last:border-0">
          <span className="w-2 h-2 rounded-full bg-[#00d4ff] flex-shrink-0" />
          <span className="text-[#e8e8ff] font-mono text-xs flex-1">{s.name}</span>
          <span className="text-[#4a4a6a] text-xs font-mono">
            {s.in?.toLocaleString()} <ChevronRight size={10} className="inline" /> {s.out?.toLocaleString()}
          </span>
          <span className="text-[#8888aa] text-xs hidden sm:block">{s.note}</span>
        </div>
      ))}
    </div>
  )
}

function QualityChecklist({ checks }) {
  const statusIcon = (s) => {
    if (s === 'pass') return <span className="text-[#00ff88]">✓</span>
    if (s === 'warn') return <span className="text-[#ffaa00]">⚠</span>
    return <span className="text-[#ff4466]">✗</span>
  }
  return (
    <div className="grid grid-cols-1 gap-0.5 max-h-64 overflow-y-auto">
      {checks?.map((c, i) => (
        <div key={i} className="flex items-center gap-2 py-1 text-xs font-mono border-b border-[#1e1e3f10] last:border-0">
          {statusIcon(c.status)}
          <span className={`flex-1 truncate ${c.status === 'pass' ? 'text-[#8888aa]' : 'text-[#e8e8ff]'}`}>
            {c.name}
          </span>
          <span className="text-[#4a4a6a]">{c.actual}</span>
        </div>
      ))}
    </div>
  )
}

function DoneOverlay({ stats, duration, onViewDashboard, onRunAgain }) {
  return (
    <div className="absolute inset-0 flex flex-col items-center justify-center bg-[#07071a] z-10 animate-fade-in rounded-2xl">
      <div className="w-20 h-20 rounded-full bg-[#00ff8820] border-2 border-[#00ff88] flex items-center justify-center mb-6 animate-pulse-glow-green">
        <CheckCircle size={40} className="text-[#00ff88]" />
      </div>
      <h2 className="text-3xl font-bold text-[#e8e8ff] mb-2">Pipeline Complete</h2>
      <p className="text-[#8888aa] font-mono mb-8">{duration}s · {stats?.ge_passed}/{stats?.ge_checks} checks passed</p>

      <div className="grid grid-cols-3 gap-4 mb-8 w-full max-w-lg px-4">
        {[
          { label: 'Orders', value: stats?.orders_normalised },
          { label: 'GPS Pings', value: stats?.gps_pings },
          { label: 'Identities', value: stats?.identity_resolved },
          { label: 'Anomalies', value: stats?.sensor_anomalies },
          { label: 'Gold Rows', value: stats?.total_gold_rows },
          { label: 'GE Pass', value: stats?.ge_passed },
        ].map((s) => (
          <div key={s.label} className="gk-card p-3 text-center">
            <div className="text-lg font-bold text-[#00d4ff]">{s.value?.toLocaleString()}</div>
            <div className="text-xs text-[#4a4a6a]">{s.label}</div>
          </div>
        ))}
      </div>

      <div className="flex gap-3">
        <button
          onClick={onViewDashboard}
          className="px-6 py-3 rounded-xl bg-[#00ff88] text-[#07071a] font-bold text-sm hover:bg-[#00ff8899] transition-colors flex items-center gap-2"
        >
          View Dashboard <ArrowDown size={16} />
        </button>
        <button
          onClick={onRunAgain}
          className="px-6 py-3 rounded-xl border border-[#1e1e3f] text-[#8888aa] font-medium text-sm hover:text-[#e8e8ff] hover:border-[#8888aa] transition-colors flex items-center gap-2"
        >
          <RotateCcw size={14} /> Run Again
        </button>
      </div>
    </div>
  )
}

export default function PipelineOrchestrator() {
  const [phase, setPhase] = useState('idle') // idle | running | done
  const [stageEvents, setStageEvents] = useState({})
  const [activeStage, setActiveStage] = useState(null)
  const [terminalLines, setTerminalLines] = useState([])
  const [doneData, setDoneData] = useState(null)
  const terminalRef = useRef(null)
  const stopStreamRef = useRef(null)

  const addTerminalLine = useCallback((line, color = '#8888aa') => {
    setTerminalLines((prev) => [...prev.slice(-80), { line, color, id: Date.now() + Math.random() }])
    setTimeout(() => {
      if (terminalRef.current) {
        terminalRef.current.scrollTop = terminalRef.current.scrollHeight
      }
    }, 10)
  }, [])

  const handleRun = async () => {
    setPhase('running')
    setStageEvents({})
    setActiveStage('GENERATE')
    setTerminalLines([])
    addTerminalLine('$ ghostkitchen run-pipeline --env production', '#00d4ff')
    addTerminalLine('Connecting to Railway PostgreSQL ...', '#4a4a6a')

    try {
      const { run_id } = await triggerRun()
      addTerminalLine(`Run ID: ${run_id.slice(0, 8)}...`, '#9945ff')

      stopStreamRef.current = streamRun(
        run_id,
        (event) => {
          if (event.stage === 'DONE') {
            setDoneData(event)
            setPhase('done')
            addTerminalLine('✓ Pipeline completed successfully', '#00ff88')
            return
          }
          if (event.stage === 'ERROR') {
            setPhase('idle')
            addTerminalLine(`✗ Error: ${event.error}`, '#ff4466')
            return
          }

          setActiveStage(event.stage)
          setStageEvents((prev) => ({ ...prev, [event.stage]: event }))

          // Terminal logging
          if (event.logs?.length) {
            event.logs.forEach((l) => addTerminalLine(`  ${l}`, '#8888aa'))
          }
          if (event.status === 'done') {
            addTerminalLine(
              `✓ ${event.stage} complete (${event.duration_s}s)`,
              event.stage === 'GOLD' ? '#ffaa00' : '#00d4ff'
            )
          }
        },
        () => {
          if (phase !== 'done') setPhase('idle')
        }
      )
    } catch (err) {
      setPhase('idle')
      addTerminalLine(`✗ Failed to connect: ${err.message}`, '#ff4466')
    }
  }

  useEffect(() => {
    return () => stopStreamRef.current?.()
  }, [])

  const currentEvent = activeStage ? stageEvents[activeStage] : null
  const doneStages = new Set(
    Object.entries(stageEvents)
      .filter(([, e]) => e.status === 'done')
      .map(([k]) => k)
  )

  return (
    <section id="pipeline" className="min-h-screen dot-grid flex flex-col items-center justify-center py-24 px-4 relative">
      {/* Radial glow */}
      <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
        <div className="w-[600px] h-[600px] rounded-full bg-[#00d4ff08] blur-3xl" />
      </div>

      {phase === 'idle' && (
        <div className="relative z-10 flex flex-col items-center text-center max-w-2xl">
          {/* Badge */}
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full border border-[#00d4ff30] bg-[#00d4ff08] text-xs font-mono text-[#00d4ff] mb-8">
            <span className="w-1.5 h-1.5 rounded-full bg-[#00d4ff] animate-live-pulse inline-block" />
            ⚡ Lambda Architecture · DuckDB · PostgreSQL · Data Vault 2.0
          </div>

          <h1 className="text-5xl sm:text-6xl font-bold text-[#e8e8ff] mb-4 leading-tight">
            Pipeline<br />
            <span className="text-[#00d4ff]">Orchestrator</span>
          </h1>

          {/* Stats */}
          <div className="flex flex-wrap justify-center gap-6 mb-8 text-sm font-mono">
            {[
              { v: '10,550', l: 'events' }, { v: '50', l: 'kitchens' },
              { v: '47', l: 'assertions' }, { v: '15', l: 'SQL views' },
            ].map(({ v, l }) => (
              <div key={l} className="text-center">
                <div className="text-xl font-bold text-[#00d4ff]">{v}</div>
                <div className="text-[#4a4a6a] text-xs">{l}</div>
              </div>
            ))}
          </div>

          {/* CTA */}
          <button
            onClick={handleRun}
            className="group relative px-10 py-5 rounded-2xl bg-[#00d4ff] text-[#07071a] font-bold text-lg hover:bg-[#00d4ffcc] transition-all duration-200 animate-pulse-glow flex items-center gap-3 mb-4"
          >
            <Play size={22} className="group-hover:scale-110 transition-transform" />
            RUN PIPELINE
          </button>
          <p className="text-[#4a4a6a] text-sm font-mono">
            Executes a full data pipeline in the cloud · ~60 seconds · Results live for 48h
          </p>
          <p className="text-[#4a4a6a] text-xs mt-2">
            Production: Apache Spark 3.5 + Delta Lake + Kafka · Demo: DuckDB + Python (identical data model)
          </p>
        </div>
      )}

      {(phase === 'running' || phase === 'done') && (
        <div className="relative z-10 w-full max-w-6xl">
          {phase === 'done' && (
            <DoneOverlay
              stats={doneData?.stats}
              duration={doneData?.duration_s}
              onViewDashboard={() => document.getElementById('dashboard')?.scrollIntoView({ behavior: 'smooth' })}
              onRunAgain={() => { setPhase('idle'); setDoneData(null) }}
            />
          )}

          <div className={phase === 'done' ? 'invisible' : ''}>
            <div className="flex flex-col lg:flex-row gap-4" style={{ minHeight: 520 }}>
              {/* LEFT: Stage sidebar */}
              <div className="lg:w-64 flex-shrink-0">
                <div className="gk-card p-4 h-full">
                  <div className="text-xs text-[#4a4a6a] font-mono uppercase tracking-wider mb-4">Stages</div>
                  {STAGES.map((stage, i) => (
                    <StageRow
                      key={stage.key}
                      stage={stage}
                      stageData={stageEvents[stage.key]}
                      isActive={activeStage === stage.key && !doneStages.has(stage.key)}
                      isDone={doneStages.has(stage.key)}
                      index={i}
                    />
                  ))}
                </div>
              </div>

              {/* RIGHT: Main content */}
              <div className="flex-1 flex flex-col gap-4">
                {/* Stage header */}
                {activeStage && (
                  <div className="gk-card p-5">
                    {(() => {
                      const s = STAGES.find((x) => x.key === activeStage)
                      const e = currentEvent
                      return (
                        <>
                          <div className="flex items-center justify-between mb-4">
                            <div className="flex items-center gap-3">
                              <span className="text-2xl">{s?.icon}</span>
                              <div>
                                <h3 className="text-lg font-bold" style={{ color: s?.color }}>{s?.label}</h3>
                                <p className="text-xs text-[#8888aa]">{s?.sub}</p>
                              </div>
                            </div>
                            {e?.status === 'running' && (
                              <div className="flex items-center gap-2 text-xs text-[#8888aa] font-mono">
                                <Loader size={12} className="animate-spin" style={{ color: s?.color }} />
                                processing...
                              </div>
                            )}
                          </div>

                          {/* Metrics grid */}
                          {e?.metrics && (
                            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
                              {Object.entries(e.metrics).slice(0, 4).map(([k, v], i) => (
                                <MetricCard
                                  key={k}
                                  label={k.replace(/_/g, ' ')}
                                  value={v}
                                  color={s?.color}
                                  delay={i * 100}
                                />
                              ))}
                            </div>
                          )}

                          {/* Stage-specific content */}
                          {activeStage === 'GENERATE' && e?.sample_json && (
                            <div className="terminal h-40">
                              <div className="terminal-header">
                                <div className="terminal-dot bg-[#ff4466]" />
                                <div className="terminal-dot bg-[#ffaa00]" />
                                <div className="terminal-dot bg-[#00ff88]" />
                                <span className="text-xs text-[#4a4a6a] ml-2">kafka events preview</span>
                              </div>
                              <div className="terminal-body h-32">
                                <JsonFeed samples={e.sample_json} />
                              </div>
                            </div>
                          )}

                          {activeStage === 'SILVER' && e?.sub_stages && (
                            <SubStageTable subStages={e.sub_stages} />
                          )}

                          {activeStage === 'QUALITY' && e?.checks && (
                            <QualityChecklist checks={e.checks} />
                          )}
                        </>
                      )
                    })()}
                  </div>
                )}

                {/* Terminal */}
                <div className="terminal flex-1" style={{ minHeight: 160 }}>
                  <div className="terminal-header">
                    <div className="terminal-dot bg-[#ff4466]" />
                    <div className="terminal-dot bg-[#ffaa00]" />
                    <div className="terminal-dot bg-[#00ff88]" />
                    <span className="text-xs text-[#4a4a6a] ml-2 font-mono">ghostkitchen.railway.app — pipeline.log</span>
                  </div>
                  <div className="terminal-body h-36" ref={terminalRef}>
                    {terminalLines.map(({ line, color, id }) => (
                      <div key={id} className="py-0.5 leading-relaxed" style={{ color }}>
                        {line}
                      </div>
                    ))}
                    <span className="animate-blink text-[#00d4ff]">█</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </section>
  )
}
