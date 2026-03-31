import { useState, useRef, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { Play, CheckCircle, AlertCircle, Loader, ChevronRight, RotateCcw, ArrowRight } from 'lucide-react'
import { triggerRun, streamRun } from '../lib/api'

const STAGES = [
  { key: 'GENERATE', label: 'Generate', sub: 'Faker · 500 orders · 8k GPS pings', color: '#00C2FF', icon: '⚡' },
  { key: 'BRONZE', label: 'Bronze', sub: 'Raw ingest → PostgreSQL', color: '#7C5CFC', icon: '🥉' },
  { key: 'SILVER', label: 'Silver', sub: 'Data Vault 2.0 · Identity Resolution', color: '#00C2FF', icon: '🥈' },
  { key: 'GOLD', label: 'Gold', sub: 'Star Schema · 8 dims · 4 facts', color: '#FFB547', icon: '🥇' },
  { key: 'QUALITY', label: 'Quality', sub: 'Great Expectations · 35 assertions', color: '#00E5A0', icon: '✓' },
]

function StageRow({ stage, stageData, isActive, isDone, index }) {
  const color = stage.color
  return (
    <div className="animate-stage-in" style={{ animationDelay: `${index * 80}ms` }}>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 10,
          padding: '10px 12px',
          borderRadius: 8,
          transition: 'all 0.3s',
          background: isActive ? 'rgba(0, 194, 255, 0.05)' : 'transparent',
          border: `1px solid ${isActive ? 'rgba(0, 194, 255, 0.15)' : 'transparent'}`,
        }}
      >
        <div style={{ width: 24, height: 24, flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          {isDone ? (
            <CheckCircle size={18} style={{ color }} />
          ) : isActive ? (
            <Loader size={18} style={{ color }} className="animate-spin" />
          ) : (
            <div style={{ width: 16, height: 16, borderRadius: '50%', border: '2px solid #142038' }} />
          )}
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{
            fontSize: 13,
            fontWeight: 600,
            fontFamily: 'Inter, sans-serif',
            color: isDone || isActive ? '#D4E5FF' : '#2D4060',
            transition: 'color 0.3s',
          }}>
            {stage.icon} {stage.label}
          </div>
          <div style={{ fontSize: 10, color: '#2D4060', fontFamily: "'JetBrains Mono', monospace", marginTop: 1, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
            {stage.sub}
          </div>
        </div>
        {isDone && stageData?.duration_s && (
          <span style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#2D4060', flexShrink: 0 }}>
            {stageData.duration_s}s
          </span>
        )}
      </div>
      {index < STAGES.length - 1 && (
        <div
          style={{
            width: 2,
            height: 14,
            marginLeft: 23,
            marginTop: 2,
            marginBottom: 2,
            borderRadius: 2,
            transition: 'all 0.5s',
            background: isDone ? color : '#142038',
            opacity: isDone ? 1 : 0.3,
          }}
        />
      )}
    </div>
  )
}

function MetricCard({ label, value, unit, color = '#00C2FF', delay = 0 }) {
  return (
    <div
      className="gk-card p-4 animate-count-up"
      style={{ animationDelay: `${delay}ms`, borderColor: `${color}30` }}
    >
      <div style={{ fontSize: 10, color: '#6B82A8', marginBottom: 4, fontFamily: "'JetBrains Mono', monospace", textTransform: 'uppercase', letterSpacing: '0.08em' }}>
        {label}
      </div>
      <div style={{ fontSize: 22, fontWeight: 700, fontFamily: 'Inter, sans-serif', color }}>
        {typeof value === 'number' ? value.toLocaleString() : value}
        {unit && <span style={{ fontSize: 12, color: '#6B82A8', marginLeft: 4 }}>{unit}</span>}
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
    return json.split('\n').map((line, j) => ({ line, color: i % 2 === 0 ? '#00C2FF' : '#7C5CFC', key: `${i}-${j}` }))
  })

  const doubled = [...lines, ...lines]

  return (
    <div style={{ overflow: 'hidden', height: '100%', position: 'relative' }}>
      <div className="animate-scroll-up">
        {doubled.map((item, idx) => (
          <div key={idx} style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, padding: '2px 4px', lineHeight: 1.6, color: item.color }}>
            {item.line}
          </div>
        ))}
      </div>
      <div style={{ position: 'absolute', bottom: 0, left: 0, right: 0, height: 40, background: 'linear-gradient(to top, #020609, transparent)', pointerEvents: 'none' }} />
    </div>
  )
}

function SubStageTable({ subStages }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {subStages?.map((s, i) => (
        <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 12, padding: '6px 0', borderBottom: i < subStages.length - 1 ? '1px solid #142038' : 'none' }}>
          <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#00C2FF', flexShrink: 0 }} />
          <span style={{ color: '#D4E5FF', fontFamily: "'JetBrains Mono', monospace", fontSize: 11, flex: 1 }}>{s.name}</span>
          <span style={{ color: '#2D4060', fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
            {s.in?.toLocaleString()} <ChevronRight size={9} style={{ display: 'inline' }} /> {s.out?.toLocaleString()}
          </span>
          <span style={{ color: '#6B82A8', fontSize: 10, display: window.innerWidth > 640 ? 'block' : 'none' }}>{s.note}</span>
        </div>
      ))}
    </div>
  )
}

function QualityChecklist({ checks }) {
  const statusIcon = (s) => {
    if (s === 'pass') return <span style={{ color: '#00E5A0' }}>✓</span>
    if (s === 'warn') return <span style={{ color: '#FFB547' }}>⚠</span>
    return <span style={{ color: '#FF3D57' }}>✗</span>
  }
  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 1, maxHeight: 240, overflowY: 'auto' }}>
      {checks?.map((c, i) => (
        <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0', fontSize: 11, fontFamily: "'JetBrains Mono', monospace", borderBottom: '1px solid rgba(20, 32, 56, 0.5)' }}>
          {statusIcon(c.status)}
          <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: c.status === 'pass' ? '#6B82A8' : '#D4E5FF' }}>
            {c.name}
          </span>
          <span style={{ color: '#2D4060' }}>{c.actual}</span>
        </div>
      ))}
    </div>
  )
}

function DoneOverlay({ stats, duration, onViewDashboard, onRunAgain }) {
  return (
    <div style={{
      position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column',
      alignItems: 'center', justifyContent: 'center', background: 'rgba(4, 9, 18, 0.97)',
      zIndex: 10, borderRadius: 16,
    }} className="animate-fade-in">
      <div
        style={{
          width: 72, height: 72, borderRadius: '50%',
          background: 'rgba(0, 229, 160, 0.1)',
          border: '2px solid #00E5A0',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          marginBottom: 20,
        }}
        className="animate-pulse-glow-green"
      >
        <CheckCircle size={36} style={{ color: '#00E5A0' }} />
      </div>
      <h2 style={{ fontFamily: 'Inter, sans-serif', fontSize: 28, fontWeight: 800, color: '#D4E5FF', marginBottom: 6 }}>
        Pipeline Complete
      </h2>
      <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: '#6B82A8', marginBottom: 28 }}>
        {duration}s · {stats?.ge_passed}/{stats?.ge_checks} checks passed
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, marginBottom: 28, width: '100%', maxWidth: 480, padding: '0 16px' }}>
        {[
          { label: 'Orders', value: stats?.orders_normalised },
          { label: 'GPS Pings', value: stats?.gps_pings },
          { label: 'Identities', value: stats?.identity_resolved },
          { label: 'Anomalies', value: stats?.sensor_anomalies },
          { label: 'Gold Rows', value: stats?.total_gold_rows },
          { label: 'GE Pass', value: stats?.ge_passed },
        ].map((s) => (
          <div key={s.label} className="gk-card" style={{ padding: '10px 12px', textAlign: 'center' }}>
            <div style={{ fontSize: 18, fontWeight: 700, fontFamily: 'Inter, sans-serif', color: '#00C2FF' }}>
              {s.value?.toLocaleString()}
            </div>
            <div style={{ fontSize: 10, color: '#2D4060', fontFamily: "'JetBrains Mono', monospace", marginTop: 2 }}>{s.label}</div>
          </div>
        ))}
      </div>

      <div style={{ display: 'flex', gap: 10 }}>
        <button
          onClick={onViewDashboard}
          style={{
            padding: '12px 24px', borderRadius: 10, background: '#00E5A0',
            color: '#040912', fontWeight: 700, fontSize: 13, fontFamily: 'Inter, sans-serif',
            border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8,
            transition: 'opacity 0.2s',
          }}
          onMouseEnter={(e) => { e.target.style.opacity = '0.85' }}
          onMouseLeave={(e) => { e.target.style.opacity = '1' }}
        >
          View Dashboard <ArrowRight size={15} />
        </button>
        <button
          onClick={onRunAgain}
          style={{
            padding: '12px 24px', borderRadius: 10,
            border: '1px solid #1E3254', background: 'transparent',
            color: '#6B82A8', fontSize: 13, fontFamily: 'Inter, sans-serif', fontWeight: 500,
            cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8,
            transition: 'all 0.2s',
          }}
          onMouseEnter={(e) => { e.currentTarget.style.color = '#D4E5FF'; e.currentTarget.style.borderColor = '#6B82A8' }}
          onMouseLeave={(e) => { e.currentTarget.style.color = '#6B82A8'; e.currentTarget.style.borderColor = '#1E3254' }}
        >
          <RotateCcw size={13} /> Run Again
        </button>
      </div>
    </div>
  )
}

export default function PipelineOrchestrator() {
  const navigate = useNavigate()
  const [phase, setPhase] = useState('idle')
  const phaseRef = useRef('idle')
  const [stageEvents, setStageEvents] = useState({})
  const [activeStage, setActiveStage] = useState(null)
  const [terminalLines, setTerminalLines] = useState([])
  const [doneData, setDoneData] = useState(null)
  const terminalRef = useRef(null)
  const stopStreamRef = useRef(null)

  const setPhaseSync = useCallback((p) => {
    phaseRef.current = p
    setPhase(p)
  }, [])

  const addTerminalLine = useCallback((line, color = '#6B82A8') => {
    setTerminalLines((prev) => [...prev.slice(-80), { line, color, id: Date.now() + Math.random() }])
    setTimeout(() => {
      if (terminalRef.current) {
        terminalRef.current.scrollTop = terminalRef.current.scrollHeight
      }
    }, 10)
  }, [])

  const handleRun = async () => {
    setPhaseSync('running')
    setStageEvents({})
    setActiveStage('GENERATE')
    setTerminalLines([])
    addTerminalLine('$ ghostkitchen run-pipeline --env production', '#00C2FF')
    addTerminalLine('Connecting to Railway PostgreSQL ...', '#2D4060')

    try {
      const { run_id } = await triggerRun()
      addTerminalLine(`Run ID: ${run_id.slice(0, 8)}...`, '#7C5CFC')

      stopStreamRef.current = streamRun(
        run_id,
        (event) => {
          if (event.stage === 'DONE') {
            setDoneData(event)
            setPhaseSync('done')
            addTerminalLine('✓ Pipeline completed successfully', '#00E5A0')
            return
          }
          if (event.stage === 'ERROR') {
            setPhaseSync('idle')
            addTerminalLine(`✗ Error: ${event.error}`, '#FF3D57')
            return
          }

          setActiveStage(event.stage)
          setStageEvents((prev) => ({ ...prev, [event.stage]: event }))

          if (event.logs?.length) {
            event.logs.forEach((l) => addTerminalLine(`  ${l}`, '#6B82A8'))
          }
          if (event.status === 'done') {
            addTerminalLine(
              `✓ ${event.stage} complete (${event.duration_s}s)`,
              event.stage === 'GOLD' ? '#FFB547' : '#00C2FF'
            )
          }
        },
        () => {
          if (phaseRef.current !== 'done') setPhaseSync('idle')
        }
      )
    } catch (err) {
      setPhaseSync('idle')
      addTerminalLine(`✗ Failed to connect: ${err.message}`, '#FF3D57')
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
    <div className="screen dot-grid" style={{ position: 'relative' }}>
      {/* Background glow */}
      <div style={{
        position: 'absolute', inset: 0, display: 'flex', alignItems: 'center',
        justifyContent: 'center', pointerEvents: 'none',
      }}>
        <div style={{ width: 600, height: 600, borderRadius: '50%', background: 'rgba(0, 194, 255, 0.04)', filter: 'blur(80px)' }} />
      </div>

      {/* IDLE */}
      {phase === 'idle' && (
        <div style={{
          flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center',
          justifyContent: 'center', padding: '0 16px', position: 'relative', zIndex: 1,
        }}>
          {/* Badge row */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, justifyContent: 'center', marginBottom: 28 }}>
            {['Lambda Architecture', 'DuckDB', 'PostgreSQL', 'Data Vault 2.0'].map((b) => (
              <span key={b} style={{
                display: 'inline-flex', alignItems: 'center', gap: 6,
                padding: '4px 12px', borderRadius: 20,
                border: '1px solid rgba(0, 194, 255, 0.2)',
                background: 'rgba(0, 194, 255, 0.05)',
                fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#00C2FF',
              }}>
                {b}
              </span>
            ))}
          </div>

          <h1 style={{
            fontFamily: 'Inter, sans-serif', fontWeight: 800, fontSize: 'clamp(40px, 7vw, 68px)',
            color: '#D4E5FF', textAlign: 'center', lineHeight: 1.05, marginBottom: 12,
          }}>
            Pipeline<br />
            <span style={{ color: '#00C2FF' }}>Orchestrator</span>
          </h1>
          <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: '#6B82A8', textAlign: 'center', marginBottom: 6, maxWidth: 480 }}>
            Bronze → Silver → Gold · Data Vault 2.0 · Star Schema · Identity Resolution
          </p>
          <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#2D4060', textAlign: 'center', marginBottom: 28, maxWidth: 520 }}>
            Modelled on the Texas ghost kitchen market — 50 virtual dark kitchens across Houston, Dallas, Austin and 7 other TX cities
          </p>

          {/* Stats strip */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 32, justifyContent: 'center', marginBottom: 32 }}>
            {[
              { v: '11,847', l: 'events / run' },
              { v: '50', l: 'TX kitchens' },
              { v: '43', l: 'DQ checks' },
              { v: '12', l: 'Gold tables' },
            ].map(({ v, l }) => (
              <div key={l} style={{ textAlign: 'center' }}>
                <div style={{ fontFamily: 'Inter, sans-serif', fontSize: 24, fontWeight: 800, color: '#00C2FF' }}>{v}</div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#2D4060', textTransform: 'uppercase', letterSpacing: '0.08em', marginTop: 2 }}>{l}</div>
              </div>
            ))}
          </div>

          <button
            onClick={handleRun}
            className="animate-pulse-glow"
            style={{
              padding: '16px 40px', borderRadius: 14,
              background: '#00C2FF', color: '#040912',
              fontFamily: 'Inter, sans-serif', fontWeight: 800, fontSize: 16,
              border: 'none', cursor: 'pointer',
              display: 'flex', alignItems: 'center', gap: 10,
              transition: 'opacity 0.2s', letterSpacing: '0.02em',
              marginBottom: 14,
            }}
            onMouseEnter={(e) => { e.currentTarget.style.opacity = '0.9' }}
            onMouseLeave={(e) => { e.currentTarget.style.opacity = '1' }}
          >
            <Play size={20} /> RUN PIPELINE
          </button>
          <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#2D4060', textAlign: 'center' }}>
            Executes a full data pipeline in the cloud · ~60 seconds · Results live for 48h
          </p>
          <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#2D4060', textAlign: 'center', marginTop: 4 }}>
            Demo engine: DuckDB + Python — same medallion concepts, no JVM, fits a $5/mo container.
          </p>
          <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#1A2640', textAlign: 'center', marginTop: 3 }}>
            Would migrate to Spark 3.5 + Delta Lake at &gt;10M daily events — that threshold hasn't landed yet.
          </p>
        </div>
      )}

      {/* RUNNING / DONE */}
      {(phase === 'running' || phase === 'done') && (
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', padding: '12px', minHeight: 0, position: 'relative' }}>
          {phase === 'done' && (
            <DoneOverlay
              stats={doneData?.stats}
              duration={doneData?.duration_s}
              onViewDashboard={() => navigate('/dashboard')}
              onRunAgain={() => { setPhaseSync('idle'); setDoneData(null) }}
            />
          )}

          <div style={{ flex: 1, display: 'flex', gap: 12, minHeight: 0, visibility: phase === 'done' ? 'hidden' : 'visible' }}>
            {/* LEFT: Stage sidebar */}
            <div style={{ width: 220, flexShrink: 0 }}>
              <div className="gk-card" style={{ padding: 12, height: '100%' }}>
                <div style={{ fontSize: 10, color: '#2D4060', fontFamily: "'JetBrains Mono', monospace", textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 12 }}>
                  Pipeline Stages
                </div>
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

            {/* RIGHT: Main content + terminal */}
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 10, minWidth: 0 }}>
              {/* Stage header */}
              {activeStage && (
                <div className="gk-card" style={{ padding: 16, flexShrink: 0 }}>
                  {(() => {
                    const s = STAGES.find((x) => x.key === activeStage)
                    const e = currentEvent
                    return (
                      <>
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                            <span style={{ fontSize: 20 }}>{s?.icon}</span>
                            <div>
                              <h3 style={{ fontFamily: 'Inter, sans-serif', fontSize: 15, fontWeight: 700, color: s?.color }}>{s?.label}</h3>
                              <p style={{ fontSize: 10, color: '#6B82A8', fontFamily: "'JetBrains Mono', monospace" }}>{s?.sub}</p>
                            </div>
                          </div>
                          {e?.status === 'running' && (
                            <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#6B82A8', fontFamily: "'JetBrains Mono', monospace" }}>
                              <Loader size={11} className="animate-spin" style={{ color: s?.color }} />
                              processing...
                            </div>
                          )}
                        </div>

                        {e?.metrics && (
                          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 8, marginBottom: 12 }}>
                            {Object.entries(e.metrics).slice(0, 4).map(([k, v], i) => (
                              <MetricCard key={k} label={k.replace(/_/g, ' ')} value={v} color={s?.color} delay={i * 100} />
                            ))}
                          </div>
                        )}

                        {activeStage === 'GENERATE' && e?.sample_json && (
                          <div className="terminal" style={{ height: 120 }}>
                            <div className="terminal-header">
                              <div className="terminal-dot" style={{ background: '#FF3D57' }} />
                              <div className="terminal-dot" style={{ background: '#FFB547' }} />
                              <div className="terminal-dot" style={{ background: '#00E5A0' }} />
                              <span style={{ fontSize: 10, color: '#2D4060', marginLeft: 8 }}>kafka events preview</span>
                            </div>
                            <div className="terminal-body" style={{ height: 88 }}>
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
              <div className="terminal" style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
                <div className="terminal-header">
                  <div className="terminal-dot" style={{ background: '#FF3D57' }} />
                  <div className="terminal-dot" style={{ background: '#FFB547' }} />
                  <div className="terminal-dot" style={{ background: '#00E5A0' }} />
                  <span style={{ fontSize: 10, color: '#2D4060', marginLeft: 8 }}>ghostkitchen.railway.app — pipeline.log</span>
                </div>
                <div className="terminal-body" style={{ flex: 1, overflowY: 'auto' }} ref={terminalRef}>
                  {terminalLines.map(({ line, color, id }) => (
                    <div key={id} style={{ padding: '2px 0', lineHeight: 1.6, color }}>
                      {line}
                    </div>
                  ))}
                  <span className="animate-blink" style={{ color: '#00C2FF' }}>█</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
