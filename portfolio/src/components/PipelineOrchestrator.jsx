import { useState, useRef, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { Play, CheckCircle, AlertCircle, Loader, ChevronRight, RotateCcw, ArrowRight } from 'lucide-react'
import { triggerRun, streamRun } from '../lib/api'

function KitchenCanvas() {
  const canvasRef = useRef(null)
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    let animId

    const dpr = window.devicePixelRatio || 1
    const resize = () => {
      const w = canvas.offsetWidth
      const h = canvas.offsetHeight
      canvas.width = w * dpr
      canvas.height = h * dpr
      ctx.scale(dpr, dpr)
    }
    resize()
    window.addEventListener('resize', resize)

    const nodePositions = [
      [0.12, 0.28], [0.26, 0.15], [0.44, 0.10], [0.62, 0.18], [0.78, 0.30],
      [0.84, 0.52], [0.70, 0.70], [0.50, 0.78], [0.30, 0.68], [0.14, 0.52],
      [0.37, 0.38], [0.60, 0.42],
    ]
    const edges = [
      [0,1],[1,2],[2,3],[3,4],[4,5],[5,6],[6,7],[7,8],[8,9],[9,0],
      [0,10],[2,10],[4,11],[6,11],[10,11],[1,9],[3,5],[7,10],
    ]
    const particles = edges.flatMap(([a, b], i) =>
      Array.from({ length: 2 }, (_, j) => ({
        a, b, t: ((i * 0.37 + j * 0.52) % 1),
        speed: 0.0015 + Math.random() * 0.0018,
      }))
    )

    const draw = () => {
      const w = canvas.offsetWidth
      const h = canvas.offsetHeight
      ctx.clearRect(0, 0, w, h)
      const nodes = nodePositions.map(([px, py]) => ({ x: px * w, y: py * h }))

      // Edges
      ctx.strokeStyle = 'rgba(191,149,63,0.07)'
      ctx.lineWidth = 1
      for (const [a, b] of edges) {
        ctx.beginPath()
        ctx.moveTo(nodes[a].x, nodes[a].y)
        ctx.lineTo(nodes[b].x, nodes[b].y)
        ctx.stroke()
      }

      // Nodes
      for (const n of nodes) {
        const grd = ctx.createRadialGradient(n.x, n.y, 0, n.x, n.y, 10)
        grd.addColorStop(0, 'rgba(191,149,63,0.15)')
        grd.addColorStop(1, 'rgba(191,149,63,0)')
        ctx.beginPath(); ctx.arc(n.x, n.y, 10, 0, Math.PI * 2)
        ctx.fillStyle = grd; ctx.fill()
        ctx.beginPath(); ctx.arc(n.x, n.y, 2.5, 0, Math.PI * 2)
        ctx.fillStyle = 'rgba(191,149,63,0.45)'; ctx.fill()
      }

      // Particles
      for (const p of particles) {
        p.t = (p.t + p.speed) % 1
        const na = nodes[p.a], nb = nodes[p.b]
        const x = na.x + (nb.x - na.x) * p.t
        const y = na.y + (nb.y - na.y) * p.t
        const g = ctx.createRadialGradient(x, y, 0, x, y, 7)
        g.addColorStop(0, 'rgba(191,149,63,0.65)')
        g.addColorStop(1, 'rgba(191,149,63,0)')
        ctx.beginPath(); ctx.arc(x, y, 7, 0, Math.PI * 2)
        ctx.fillStyle = g; ctx.fill()
        ctx.beginPath(); ctx.arc(x, y, 1.8, 0, Math.PI * 2)
        ctx.fillStyle = 'rgba(191,149,63,0.9)'; ctx.fill()
      }

      animId = requestAnimationFrame(draw)
    }
    draw()
    return () => { cancelAnimationFrame(animId); window.removeEventListener('resize', resize) }
  }, [])

  return (
    <canvas ref={canvasRef} style={{
      position: 'absolute', inset: 0, width: '100%', height: '100%', pointerEvents: 'none',
    }} />
  )
}

const STAGES = [
  { key: 'GENERATE', label: 'Generate', sub: 'Faker · 500 orders · 8k GPS pings', color: '#BF953F', icon: '⚡' },
  { key: 'BRONZE', label: 'Bronze', sub: 'Raw ingest → PostgreSQL', color: '#7A6B52', icon: '🥉' },
  { key: 'SILVER', label: 'Silver', sub: 'Data Vault 2.0 · Identity Resolution', color: '#BF953F', icon: '🥈' },
  { key: 'GOLD', label: 'Gold', sub: 'Star Schema · 8 dims · 4 facts', color: '#D4866A', icon: '🥇' },
  { key: 'QUALITY', label: 'Quality', sub: 'Great Expectations · 35 assertions', color: '#4A7C59', icon: '✓' },
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
          background: isActive ? 'rgba(191, 149, 63, 0.06)' : 'transparent',
          border: `1px solid ${isActive ? 'rgba(191, 149, 63, 0.18)' : 'transparent'}`,
        }}
      >
        <div style={{ width: 24, height: 24, flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          {isDone ? (
            <CheckCircle size={18} style={{ color }} />
          ) : isActive ? (
            <Loader size={18} style={{ color }} className="animate-spin" />
          ) : (
            <div style={{ width: 16, height: 16, borderRadius: '50%', border: '2px solid #D9D1C4' }} />
          )}
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{
            fontSize: 13,
            fontWeight: 600,
            fontFamily: 'Inter, sans-serif',
            color: isDone || isActive ? '#1C1A16' : '#A09488',
            transition: 'color 0.3s',
          }}>
            {stage.icon} {stage.label}
          </div>
          <div style={{ fontSize: 10, color: '#A09488', fontFamily: "'JetBrains Mono', monospace", marginTop: 1, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
            {stage.sub}
          </div>
        </div>
        {isDone && stageData?.duration_s && (
          <span style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#A09488', flexShrink: 0 }}>
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
            background: isDone ? color : '#D9D1C4',
            opacity: isDone ? 1 : 0.4,
          }}
        />
      )}
    </div>
  )
}

function MetricCard({ label, value, unit, color = '#BF953F', delay = 0 }) {
  return (
    <div
      className="gk-card p-4 animate-count-up"
      style={{ animationDelay: `${delay}ms`, borderColor: `${color}30` }}
    >
      <div style={{ fontSize: 10, color: '#A09488', marginBottom: 4, fontFamily: "'JetBrains Mono', monospace", textTransform: 'uppercase', letterSpacing: '0.08em' }}>
        {label}
      </div>
      <div style={{ fontSize: 22, fontWeight: 700, fontFamily: 'Inter, sans-serif', color }}>
        {typeof value === 'number' ? value.toLocaleString() : value}
        {unit && <span style={{ fontSize: 12, color: '#A09488', marginLeft: 4 }}>{unit}</span>}
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
    return json.split('\n').map((line, j) => ({ line, color: i % 2 === 0 ? '#BF953F' : '#7A6B52', key: `${i}-${j}` }))
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
      <div style={{ position: 'absolute', bottom: 0, left: 0, right: 0, height: 40, background: 'linear-gradient(to top, #F3EFE8, transparent)', pointerEvents: 'none' }} />
    </div>
  )
}

function SubStageTable({ subStages }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {subStages?.map((s, i) => (
        <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 12, padding: '6px 0', borderBottom: i < subStages.length - 1 ? '1px solid #D9D1C4' : 'none' }}>
          <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#BF953F', flexShrink: 0 }} />
          <span style={{ color: '#1C1A16', fontFamily: "'JetBrains Mono', monospace", fontSize: 11, flex: 1 }}>{s.name}</span>
          <span style={{ color: '#A09488', fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
            {s.in?.toLocaleString()} <ChevronRight size={9} style={{ display: 'inline' }} /> {s.out?.toLocaleString()}
          </span>
          <span style={{ color: '#6B6256', fontSize: 10, display: window.innerWidth > 640 ? 'block' : 'none' }}>{s.note}</span>
        </div>
      ))}
    </div>
  )
}

function QualityChecklist({ checks }) {
  const statusIcon = (s) => {
    if (s === 'pass') return <span style={{ color: '#4A7C59' }}>✓</span>
    if (s === 'warn') return <span style={{ color: '#D4866A' }}>⚠</span>
    return <span style={{ color: '#C0614A' }}>✗</span>
  }
  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 1, maxHeight: 240, overflowY: 'auto' }}>
      {checks?.map((c, i) => (
        <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0', fontSize: 11, fontFamily: "'JetBrains Mono', monospace", borderBottom: '1px solid #D9D1C4' }}>
          {statusIcon(c.status)}
          <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: c.status === 'pass' ? '#6B6256' : '#1C1A16' }}>
            {c.name}
          </span>
          <span style={{ color: '#A09488' }}>{c.actual}</span>
        </div>
      ))}
    </div>
  )
}

function DoneOverlay({ stats, duration, onViewDashboard, onRunAgain }) {
  return (
    <div style={{
      position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column',
      alignItems: 'center', justifyContent: 'center', background: 'rgba(250, 248, 244, 0.97)',
      zIndex: 10, borderRadius: 16,
    }} className="animate-fade-in">
      <div
        style={{
          width: 72, height: 72, borderRadius: '50%',
          background: 'rgba(74, 124, 89, 0.08)',
          border: '2px solid #4A7C59',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          marginBottom: 20,
        }}
        className="animate-pulse-glow-green"
      >
        <CheckCircle size={36} style={{ color: '#4A7C59' }} />
      </div>
      <h2 style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: 'italic', fontSize: 32, fontWeight: 600, color: '#1C1A16', marginBottom: 6 }}>
        Pipeline Complete
      </h2>
      <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: '#6B6256', marginBottom: 6 }}>
        {duration}s · {stats?.ge_passed}/{stats?.ge_checks} DQ checks passed
      </p>
      <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488', marginBottom: 28 }}>
        {stats?.orders_normalised} orders unified · {stats?.identity_resolved} customers resolved · 24 PII fields masked
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, marginBottom: 28, width: '100%', maxWidth: 480, padding: '0 16px' }}>
        {[
          { label: 'Orders Unified', value: stats?.orders_normalised },
          { label: 'GPS Validated', value: stats?.gps_pings },
          { label: 'IDs Resolved', value: stats?.identity_resolved },
          { label: 'Sensor Anomalies', value: stats?.sensor_anomalies },
          { label: 'Gold Rows', value: stats?.total_gold_rows },
          { label: 'DQ Passed', value: stats?.ge_passed },
        ].map((s) => (
          <div key={s.label} className="gk-card" style={{ padding: '10px 12px', textAlign: 'center' }}>
            <div style={{ fontSize: 18, fontWeight: 700, fontFamily: 'Inter, sans-serif', color: '#BF953F' }}>
              {s.value?.toLocaleString()}
            </div>
            <div style={{ fontSize: 10, color: '#A09488', fontFamily: "'JetBrains Mono', monospace", marginTop: 2 }}>{s.label}</div>
          </div>
        ))}
      </div>

      <div style={{ display: 'flex', gap: 10 }}>
        <button
          onClick={onViewDashboard}
          style={{
            padding: '12px 24px', borderRadius: 10, background: '#BF953F',
            color: '#FAF8F4', fontWeight: 700, fontSize: 13, fontFamily: 'Inter, sans-serif',
            border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8,
            transition: 'opacity 0.2s, transform 0.2s',
            boxShadow: '0 4px 16px rgba(191,149,63,0.25)',
          }}
          onMouseEnter={(e) => { e.currentTarget.style.opacity = '0.88'; e.currentTarget.style.transform = 'translateY(-1px)' }}
          onMouseLeave={(e) => { e.currentTarget.style.opacity = '1'; e.currentTarget.style.transform = 'translateY(0)' }}
        >
          View Dashboard <ArrowRight size={15} />
        </button>
        <button
          onClick={onRunAgain}
          style={{
            padding: '12px 24px', borderRadius: 10,
            border: '1px solid #D9D1C4', background: 'transparent',
            color: '#6B6256', fontSize: 13, fontFamily: 'Inter, sans-serif', fontWeight: 500,
            cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8,
            transition: 'all 0.2s',
          }}
          onMouseEnter={(e) => { e.currentTarget.style.color = '#1C1A16'; e.currentTarget.style.borderColor = '#C4B99A' }}
          onMouseLeave={(e) => { e.currentTarget.style.color = '#6B6256'; e.currentTarget.style.borderColor = '#D9D1C4' }}
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
  const [connectError, setConnectError] = useState(null)
  const terminalRef = useRef(null)
  const stopStreamRef = useRef(null)

  const setPhaseSync = useCallback((p) => {
    phaseRef.current = p
    setPhase(p)
  }, [])

  const addTerminalLine = useCallback((line, color = '#6B6256') => {
    setTerminalLines((prev) => [...prev.slice(-80), { line, color, id: Date.now() + Math.random() }])
    setTimeout(() => {
      if (terminalRef.current) {
        terminalRef.current.scrollTop = terminalRef.current.scrollHeight
      }
    }, 10)
  }, [])

  const handleRun = async () => {
    setConnectError(null)
    setPhaseSync('running')
    setStageEvents({})
    setActiveStage('GENERATE')
    setTerminalLines([])
    addTerminalLine('$ ghostkitchen run-pipeline --env production', '#BF953F')
    addTerminalLine('Connecting to Railway PostgreSQL ...', '#A09488')

    try {
      const { run_id } = await triggerRun()
      addTerminalLine(`Run ID: ${run_id.slice(0, 8)}...`, '#7A6B52')

      stopStreamRef.current = streamRun(
        run_id,
        (event) => {
          if (event.stage === 'DONE') {
            setDoneData(event)
            setPhaseSync('done')
            addTerminalLine('✓ Pipeline completed successfully', '#4A7C59')
            return
          }
          if (event.stage === 'ERROR') {
            setPhaseSync('idle')
            addTerminalLine(`✗ Error: ${event.error}`, '#C0614A')
            return
          }

          setActiveStage(event.stage)
          setStageEvents((prev) => ({ ...prev, [event.stage]: event }))

          if (event.logs?.length) {
            event.logs.forEach((l) => addTerminalLine(`  ${l}`, '#6B6256'))
          }
          if (event.status === 'done') {
            addTerminalLine(
              `✓ ${event.stage} complete (${event.duration_s}s)`,
              event.stage === 'GOLD' ? '#D4866A' : '#BF953F'
            )
          }
        },
        () => {
          if (phaseRef.current !== 'done') setPhaseSync('idle')
        }
      )
    } catch (err) {
      setPhaseSync('idle')
      setConnectError(err.message)
      addTerminalLine(`✗ Failed to connect: ${err.message}`, '#C0614A')
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
      {/* Subtle warm ambient glow */}
      <div style={{
        position: 'absolute', inset: 0, display: 'flex', alignItems: 'center',
        justifyContent: 'center', pointerEvents: 'none',
      }}>
        <div style={{ width: 700, height: 700, borderRadius: '50%', background: 'rgba(191, 149, 63, 0.04)', filter: 'blur(100px)' }} />
      </div>

      {/* Error banner — fixed so it's always visible */}
      {connectError && (
        <div style={{
          position: 'fixed', bottom: 24, left: '50%', transform: 'translateX(-50%)',
          zIndex: 200, padding: '10px 20px', borderRadius: 8,
          background: 'rgba(250,248,244,0.97)', border: '1px solid rgba(192,97,74,0.4)',
          fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#C0614A',
          display: 'flex', alignItems: 'center', gap: 10,
          boxShadow: '0 4px 24px rgba(192,97,74,0.15)', whiteSpace: 'nowrap',
        }}>
          <AlertCircle size={13} />
          Backend unreachable — start the server and try again
          <button onClick={() => setConnectError(null)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#C0614A', padding: 0, marginLeft: 4, lineHeight: 1 }}>✕</button>
        </div>
      )}

      {/* ── IDLE: Split hero ── */}
      {phase === 'idle' && (
        <div style={{ flex: 1, display: 'flex', position: 'relative', overflow: 'hidden' }}>
          <KitchenCanvas />

          {/* LEFT — text + CTA */}
          <div style={{
            flex: '0 0 52%', display: 'flex', flexDirection: 'column', justifyContent: 'center',
            padding: '40px 32px 40px 48px', position: 'relative', zIndex: 1,
          }}>
            <p style={{
              fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488',
              textTransform: 'uppercase', letterSpacing: '0.16em', marginBottom: 20,
            }}>
              Texas · Ghost Kitchen Network
            </p>

            <h1 style={{
              fontFamily: "'Cormorant Garamond', Georgia, serif",
              fontWeight: 700, fontStyle: 'italic',
              fontSize: 'clamp(48px, 5.5vw, 78px)',
              color: '#1C1A16', lineHeight: 1.0,
              marginBottom: 18, letterSpacing: '-0.02em',
            }}>
              Fifty kitchens.<br />
              <span style={{ color: '#BF953F' }}>One pipeline.</span>
            </h1>

            <div style={{ width: 72, height: 3, background: '#BF953F', marginBottom: 24, borderRadius: 2 }} />

            <p style={{
              fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: '#6B6256',
              marginBottom: 8, lineHeight: 1.8, maxWidth: 420,
            }}>
              Three delivery platforms. Three conflicting customer records.<br />
              One pipeline to unify them — Bronze → Silver → Gold.
            </p>
            <p style={{
              fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488',
              marginBottom: 32, lineHeight: 1.7,
            }}>
              Data Vault 2.0 · Star Schema · Identity Resolution · 43 DQ checks
            </p>

            {/* Inline stats */}
            <div style={{ display: 'flex', gap: 28, marginBottom: 36, flexWrap: 'wrap' }}>
              {[
                { v: '50', l: 'kitchens' },
                { v: '12', l: 'gold tables' },
                { v: '43', l: 'DQ checks' },
                { v: '11.8k', l: 'events / run' },
              ].map(({ v, l }) => (
                <div key={l}>
                  <div style={{
                    fontFamily: 'Inter, sans-serif',
                    fontWeight: 800,
                    fontSize: 32, color: '#BF953F', lineHeight: 1,
                    fontVariantNumeric: 'tabular-nums',
                  }}>{v}</div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 9, color: '#A09488', textTransform: 'uppercase', letterSpacing: '0.1em', marginTop: 3 }}>{l}</div>
                </div>
              ))}
            </div>

            <div>
              <button
                onClick={handleRun}
                style={{
                  padding: '14px 38px', borderRadius: 10,
                  background: '#1C1A16', color: '#FAF8F4',
                  fontFamily: 'Inter, sans-serif', fontWeight: 700, fontSize: 13,
                  border: 'none', cursor: 'pointer',
                  display: 'inline-flex', alignItems: 'center', gap: 10,
                  letterSpacing: '0.06em', textTransform: 'uppercase',
                  transition: 'background 0.2s, transform 0.15s, box-shadow 0.2s',
                  boxShadow: '0 4px 20px rgba(28,26,22,0.15)',
                }}
                onMouseEnter={(e) => { e.currentTarget.style.background = '#BF953F'; e.currentTarget.style.transform = 'translateY(-2px)'; e.currentTarget.style.boxShadow = '0 8px 30px rgba(191,149,63,0.3)' }}
                onMouseLeave={(e) => { e.currentTarget.style.background = '#1C1A16'; e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = '0 4px 20px rgba(28,26,22,0.15)' }}
              >
                <Play size={16} fill="#FAF8F4" /> Run Pipeline
              </button>
              <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488', marginTop: 10 }}>
                ~60 seconds · live results · writes real rows to PostgreSQL
              </p>
            </div>
          </div>

          {/* RIGHT — architecture card */}
          <div style={{
            flex: '0 0 48%', display: 'flex', alignItems: 'center', justifyContent: 'center',
            padding: '40px 40px 40px 8px', position: 'relative', zIndex: 1,
          }}>
            <div style={{
              width: '100%', maxWidth: 380,
              border: '1px solid #D9D1C4', borderRadius: 14, overflow: 'hidden',
              boxShadow: '0 8px 48px rgba(28,26,22,0.09)',
              background: 'rgba(250,248,244,0.92)',
              backdropFilter: 'blur(8px)',
            }}>
              <div style={{
                background: '#EDE8DF', padding: '10px 16px', borderBottom: '1px solid #D9D1C4',
                fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#A09488',
                textTransform: 'uppercase', letterSpacing: '0.1em', display: 'flex', alignItems: 'center', gap: 8,
              }}>
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#BF953F' }} />
                Lambda Architecture
              </div>
              <div style={{ display: 'flex', background: 'transparent' }}>
                <div style={{ flex: 1, padding: '14px 16px', borderRight: '1px solid #D9D1C4' }}>
                  <div style={{ fontSize: 9, fontFamily: "'JetBrains Mono', monospace", color: '#BF953F', marginBottom: 10, textTransform: 'uppercase', letterSpacing: '0.1em' }}>⚡ Speed</div>
                  {['Kafka · 4 topics', 'Spark Streaming', 'Speed Tables'].map((label, i) => (
                    <div key={label}>
                      <div style={{ padding: '5px 9px', borderRadius: 6, marginBottom: 2, border: '1px solid rgba(191,149,63,0.18)', background: 'rgba(191,149,63,0.05)', fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#BF953F' }}>{label}</div>
                      {i < 2 && <div style={{ fontSize: 10, color: '#A09488', textAlign: 'center', marginBottom: 2 }}>↓</div>}
                    </div>
                  ))}
                </div>
                <div style={{ flex: 1, padding: '14px 16px' }}>
                  <div style={{ fontSize: 9, fontFamily: "'JetBrains Mono', monospace", color: '#4A7C59', marginBottom: 10, textTransform: 'uppercase', letterSpacing: '0.1em' }}>📦 Batch</div>
                  {[
                    { label: 'Airflow DAGs (5)', color: '#4A7C59' },
                    { label: 'Spark 3.5 batch', color: '#4A7C59' },
                    { label: 'Delta Lake MERGE', color: '#D4866A' },
                  ].map((item, i) => (
                    <div key={item.label}>
                      <div style={{ padding: '5px 9px', borderRadius: 6, marginBottom: 2, border: `1px solid ${item.color}22`, background: `${item.color}07`, fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: item.color }}>{item.label}</div>
                      {i < 2 && <div style={{ fontSize: 10, color: '#A09488', textAlign: 'center', marginBottom: 2 }}>↓</div>}
                    </div>
                  ))}
                </div>
              </div>
              <div style={{ padding: '8px 16px', borderTop: '1px solid #D9D1C4', background: 'rgba(237,232,223,0.8)', fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#A09488', textAlign: 'center' }}>
                Serving Layer · PostgreSQL · 12 Gold tables
              </div>

              {/* Tech badges */}
              <div style={{ padding: '10px 16px', borderTop: '1px solid #D9D1C4', display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                {['Kafka', 'Spark 3.5', 'Airflow', 'Delta Lake', 'Data Vault 2.0', 'Great Expectations'].map((t) => (
                  <span key={t} style={{ padding: '2px 9px', borderRadius: 20, fontSize: 9, fontFamily: "'JetBrains Mono', monospace", border: '1px solid #D9D1C4', color: '#A09488', background: '#F3EFE8' }}>{t}</span>
                ))}
              </div>

              {/* Design decisions */}
              <div style={{ padding: '10px 16px', borderTop: '1px solid #D9D1C4', display: 'flex', flexDirection: 'column', gap: 6 }}>
                <div style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#BF953F' }}>⚖ Lambda over Kappa</div>
                <div style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#A09488', lineHeight: 1.6 }}>Order corrections need batch reprocessing. Kappa can't replay state machines retroactively.</div>
                <div style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#BF953F', marginTop: 4 }}>⏱ 24h Late-Arriving Window</div>
                <div style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#A09488', lineHeight: 1.6 }}>Airflow reconciliation DAG at 02:00 UTC · watermark-based deduplication.</div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ── RUNNING / DONE ── */}
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
                <div style={{ fontSize: 10, color: '#A09488', fontFamily: "'JetBrains Mono', monospace", textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 12 }}>
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

            {/* RIGHT: Main content + log panel */}
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
                              <p style={{ fontSize: 10, color: '#A09488', fontFamily: "'JetBrains Mono', monospace" }}>{s?.sub}</p>
                            </div>
                          </div>
                          {e?.status === 'running' && (
                            <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#A09488', fontFamily: "'JetBrains Mono', monospace" }}>
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
                              <div className="terminal-dot" style={{ background: '#C0614A' }} />
                              <div className="terminal-dot" style={{ background: '#D4866A' }} />
                              <div className="terminal-dot" style={{ background: '#4A7C59' }} />
                              <span style={{ fontSize: 10, color: '#A09488', marginLeft: 8 }}>kafka events preview</span>
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

              {/* Elegant log panel */}
              <div className="terminal" style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
                <div className="terminal-header">
                  <div className="terminal-dot" style={{ background: '#C0614A' }} />
                  <div className="terminal-dot" style={{ background: '#D4866A' }} />
                  <div className="terminal-dot" style={{ background: '#4A7C59' }} />
                  <span style={{ fontSize: 10, color: '#A09488', marginLeft: 8 }}>ghostkitchen.railway.app — pipeline.log</span>
                </div>
                <div className="terminal-body" style={{ flex: 1, overflowY: 'auto' }} ref={terminalRef}>
                  {terminalLines.map(({ line, color, id }) => (
                    <div key={id} style={{ padding: '2px 0', lineHeight: 1.6, color }}>
                      {line}
                    </div>
                  ))}
                  <span className="animate-blink" style={{ color: '#BF953F' }}>█</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
