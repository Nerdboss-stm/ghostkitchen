import { useState, useEffect, useRef, useCallback } from 'react'
import { fetchLineage } from '../lib/api'

const LAYERS = [
  {
    id: 'sources',
    label: 'SOURCES',
    color: '#BF953F',
    glow: 'rgba(191,149,63,0.06)',
    border: 'rgba(191,149,63,0.22)',
    tables: [
      { name: 'Uber Eats API', desc: 'Order webhook stream', rows: null },
      { name: 'DoorDash API', desc: 'REST polling endpoint', rows: null },
      { name: 'Own App API', desc: 'Native order channel', rows: null },
      { name: 'IoT Sensors', desc: 'Temp · Humidity · CO2', rows: null },
      { name: 'GPS Trackers', desc: '8k pings per run', rows: null },
      { name: 'Menu CDC', desc: 'Change data capture', rows: null },
    ],
  },
  {
    id: 'bronze',
    label: 'BRONZE',
    color: '#7A6B52',
    glow: 'rgba(122,107,82,0.06)',
    border: 'rgba(122,107,82,0.22)',
    tables: [
      { name: 'bronze_orders', desc: 'Raw order JSON, no transforms', rows: null },
      { name: 'bronze_sensors', desc: 'Raw IoT readings', rows: null },
      { name: 'bronze_gps', desc: 'Raw GPS pings with timestamps', rows: null },
      { name: 'bronze_menu_cdc', desc: 'Menu change events', rows: null },
    ],
  },
  {
    id: 'silver',
    label: 'SILVER',
    color: '#4A7C59',
    glow: 'rgba(74,124,89,0.06)',
    border: 'rgba(74,124,89,0.22)',
    tables: [
      { name: 'silver_hub_order', desc: 'Data Vault hub · order_hk', rows: null },
      { name: 'silver_hub_customer', desc: 'Data Vault hub · customer_hk', rows: null },
      { name: 'silver_sat_order_details', desc: 'Satellite · order attributes', rows: null },
      { name: 'silver_identity_bridge', desc: 'SHA-256 cross-platform identity', rows: null },
    ],
  },
  {
    id: 'gold',
    label: 'GOLD',
    color: '#BF953F',
    glow: 'rgba(191,149,63,0.06)',
    border: 'rgba(191,149,63,0.22)',
    tables: [
      { name: 'dim_date', desc: 'SCD0 · Calendar attributes', rows: null },
      { name: 'dim_kitchen', desc: 'SCD1 · Kitchen master', rows: null },
      { name: 'dim_brand', desc: 'SCD0 · Brand lookup', rows: null },
      { name: 'dim_customer', desc: 'SCD2 · Customer history', rows: null },
      { name: 'fact_order', desc: 'Grain: 1 delivered order', rows: null },
      { name: 'fact_delivery_trip', desc: 'GPS haversine route', rows: null },
      { name: 'fact_sensor_hourly', desc: 'Sensor anomaly aggregates', rows: null },
      { name: 'fact_order_state_history', desc: 'Order lifecycle states', rows: null },
    ],
  },
  {
    id: 'analytics',
    label: 'ANALYTICS',
    color: '#4A7C59',
    glow: 'rgba(74,124,89,0.06)',
    border: 'rgba(74,124,89,0.22)',
    tables: [
      { name: 'Live Dashboard', desc: 'KPIs · Charts · Real-time', rows: null },
      { name: 'Kitchen Map', desc: 'Geo utilization · 50 locations', rows: null },
      { name: 'Schema Explorer', desc: 'Interactive React Flow graph', rows: null },
    ],
  },
]

function FlowArrow({ color, boosted }) {
  const canvasRef = useRef(null)
  const animRef = useRef(null)
  const particlesRef = useRef(
    Array.from({ length: 6 }, (_, i) => ({
      t: i / 6,
      speed: 0.003 + Math.random() * 0.005,
    }))
  )

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')

    const ro = new ResizeObserver(() => {
      canvas.width = 52
      canvas.height = canvas.offsetHeight || canvas.parentElement?.offsetHeight || 120
    })
    ro.observe(canvas.parentElement || canvas)
    canvas.width = 52
    canvas.height = canvas.offsetHeight || 120

    const draw = () => {
      const h = canvas.height
      ctx.clearRect(0, 0, 52, h)
      const cy = h / 2

      ctx.beginPath()
      ctx.moveTo(0, cy)
      ctx.lineTo(42, cy)
      ctx.strokeStyle = 'rgba(191,149,63,0.12)'
      ctx.lineWidth = 1
      ctx.stroke()

      const speedMult = boosted ? 2 : 1
      for (const p of particlesRef.current) {
        p.t = (p.t + p.speed * speedMult) % 1
        const x = p.t * 42
        const g = ctx.createRadialGradient(x, cy, 0, x, cy, 6)
        g.addColorStop(0, 'rgba(191,149,63,0.7)')
        g.addColorStop(1, 'rgba(191,149,63,0)')
        ctx.beginPath()
        ctx.arc(x, cy, 6, 0, Math.PI * 2)
        ctx.fillStyle = g
        ctx.fill()
        ctx.beginPath()
        ctx.arc(x, cy, 1.5, 0, Math.PI * 2)
        ctx.fillStyle = color
        ctx.fill()
      }

      ctx.beginPath()
      ctx.moveTo(42, cy - 5)
      ctx.lineTo(50, cy)
      ctx.lineTo(42, cy + 5)
      ctx.closePath()
      ctx.fillStyle = color
      ctx.fill()

      animRef.current = requestAnimationFrame(draw)
    }
    draw()

    return () => {
      cancelAnimationFrame(animRef.current)
      ro.disconnect()
    }
  }, [color, boosted])

  return (
    <canvas
      ref={canvasRef}
      width={52}
      style={{ display: 'block', width: 52, height: '100%', minHeight: 80 }}
    />
  )
}

function TableCard({ table, layerColor, layerBorder, layerGlow, layerLabel, isSelected, onClick, onLayerHover, onLayerLeave }) {
  const [hovered, setHovered] = useState(false)

  return (
    <div
      onClick={onClick}
      onMouseEnter={() => { setHovered(true); onLayerHover && onLayerHover() }}
      onMouseLeave={() => { setHovered(false); onLayerLeave && onLayerLeave() }}
      style={{
        padding: '10px 12px', borderRadius: 8, cursor: 'pointer',
        borderLeft: isSelected ? `3px solid ${layerColor}` : hovered ? `3px solid ${layerColor}` : '3px solid transparent',
        border: isSelected
          ? `1px solid ${layerColor}`
          : hovered
          ? `1px solid ${layerBorder}`
          : '1px solid #D9D1C4',
        borderLeft: isSelected ? `3px solid ${layerColor}` : hovered ? `3px solid ${layerColor}` : '3px solid transparent',
        background: isSelected ? layerGlow : hovered ? 'rgba(191,149,63,0.04)' : '#F3EFE8',
        transition: 'all 0.15s',
        boxShadow: isSelected ? `0 4px 16px ${layerGlow}` : hovered ? '0 2px 8px rgba(28,26,22,0.06)' : 'none',
        marginBottom: 6,
        position: 'relative',
      }}
    >
      {isSelected && (
        <span style={{
          position: 'absolute', top: 6, right: 6,
          width: 6, height: 6, borderRadius: '50%',
          background: layerColor,
        }} />
      )}
      {hovered && !isSelected && (
        <span style={{
          position: 'absolute', top: 4, right: 6,
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 8, fontWeight: 600, color: layerColor,
          letterSpacing: '0.06em', opacity: 0.8,
        }}>
          {layerLabel}
        </span>
      )}
      <div style={{
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: 11, fontWeight: 500,
        color: isSelected ? layerColor : '#1C1A16',
        marginBottom: 2, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
        paddingRight: hovered || isSelected ? 36 : 0,
      }}>
        {table.name}
      </div>
      <div style={{ fontSize: 9, fontFamily: "'JetBrains Mono', monospace", color: '#A09488', marginBottom: table.rows != null ? 4 : 0 }}>
        {table.desc}
      </div>
      {table.rows != null && (
        <div style={{ fontFamily: 'Inter, sans-serif', fontWeight: 700, fontSize: 13, color: layerColor }}>
          {table.rows.toLocaleString()}
          <span style={{ fontSize: 9, fontWeight: 400, color: '#A09488', marginLeft: 3 }}>rows</span>
        </div>
      )}
    </div>
  )
}

function JourneyTrace({ currentLayerLabel }) {
  const allLabels = ['SOURCES', 'BRONZE', 'SILVER', 'GOLD', 'ANALYTICS']
  const currentIdx = allLabels.indexOf(currentLayerLabel)
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 0, flexShrink: 0 }}>
      {allLabels.map((label, i) => (
        <div key={label} style={{ display: 'flex', alignItems: 'center' }}>
          {i > 0 && (
            <div style={{ width: 18, height: 1, background: i <= currentIdx ? 'rgba(191,149,63,0.5)' : '#D9D1C4', margin: '0 2px' }} />
          )}
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3 }}>
            <div style={{
              width: i === currentIdx ? 9 : 6,
              height: i === currentIdx ? 9 : 6,
              borderRadius: '50%',
              background: i === currentIdx ? '#BF953F' : i < currentIdx ? 'rgba(191,149,63,0.35)' : '#D9D1C4',
              transition: 'all 0.2s',
            }} />
            <span style={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: 8,
              color: i === currentIdx ? '#BF953F' : '#A09488',
              fontWeight: i === currentIdx ? 700 : 400,
              letterSpacing: '0.04em',
            }}>
              {label}
            </span>
          </div>
        </div>
      ))}
    </div>
  )
}

export default function DataLineage() {
  const [lineageData, setLineageData] = useState(null)
  const [selectedTable, setSelectedTable] = useState(null)
  const [lastRun, setLastRun] = useState(null)
  const [hoveredLayer, setHoveredLayer] = useState(null)

  useEffect(() => {
    const style = document.createElement('style')
    style.textContent = `
      @keyframes fadeSlideIn {
        from { opacity: 0; transform: translateY(8px); }
        to { opacity: 1; transform: translateY(0); }
      }
    `
    document.head.appendChild(style)
    return () => style.remove()
  }, [])

  useEffect(() => {
    fetchLineage()
      .then((data) => {
        setLineageData(data)
        setLastRun(new Date().toLocaleTimeString())
      })
      .catch(() => {})
  }, [])

  const layers = LAYERS.map((layer) => ({
    ...layer,
    tables: layer.tables.map((t) => {
      if (!lineageData) return t
      const key = t.name.toLowerCase().replace(/\s+/g, '_')
      const rowCount = lineageData[key] ?? lineageData[t.name] ?? null
      return { ...t, rows: typeof rowCount === 'number' ? rowCount : t.rows }
    }),
  }))

  const allTables = layers.flatMap((l) =>
    l.tables.map((t) => ({ ...t, layerColor: l.color, layerLabel: l.label }))
  )
  const selectedInfo = selectedTable
    ? allTables.find((t) => t.name === selectedTable)
    : null

  return (
    <div className="screen" style={{ padding: '16px 20px', gap: 12 }}>
      {/* Header */}
      <div style={{ flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div>
          <h2 style={{
            fontFamily: "'Cormorant Garamond', Georgia, serif",
            fontStyle: 'italic',
            fontWeight: 600,
            fontSize: 26,
            color: '#1C1A16',
            marginBottom: 2,
          }}>
            Data Lineage
          </h2>
          <p style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488' }}>
            Sources → Bronze → Silver → Gold → Analytics · End-to-end data provenance
          </p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488' }}>
          {lastRun && <span>Last run: {lastRun}</span>}
          <span style={{
            display: 'flex', alignItems: 'center', gap: 5, padding: '3px 10px', borderRadius: 20,
            border: '1px solid #D9D1C4', color: '#A09488',
          }}>
            5 layers · 25 tables
          </span>
        </div>
      </div>

      {/* Main flow area */}
      <div style={{ flex: 1, minHeight: 0, overflowX: 'auto', overflowY: 'hidden' }}>
        <div style={{
          display: 'flex', alignItems: 'stretch', gap: 0,
          minWidth: 'max-content', height: '100%', paddingBottom: 4,
        }}>
          {layers.map((layer, lIdx) => {
            const isHovered = hoveredLayer === layer.id
            const otherHovered = hoveredLayer !== null && !isHovered
            return (
              <div key={layer.id} style={{ display: 'flex', alignItems: 'stretch' }}>
                {/* Layer column */}
                <div style={{
                  width: 196, display: 'flex', flexDirection: 'column',
                  opacity: 0,
                  animation: 'fadeSlideIn 0.4s ease forwards',
                  animationDelay: `${lIdx * 80}ms`,
                }}>
                  {/* Layer header */}
                  <div style={{
                    padding: '6px 10px', marginBottom: 8, borderRadius: 6,
                    border: `1px solid ${isHovered ? 'rgba(191,149,63,0.4)' : layer.border}`,
                    background: isHovered ? 'rgba(191,149,63,0.12)' : layer.glow,
                    display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0,
                    opacity: otherHovered ? 0.6 : 1,
                    transition: 'all 0.2s',
                  }}>
                    <span style={{ width: 6, height: 6, borderRadius: '50%', background: layer.color, flexShrink: 0 }} />
                    <span style={{
                      fontFamily: "'JetBrains Mono', monospace",
                      fontWeight: 600, fontSize: 10, color: layer.color,
                      textTransform: 'uppercase', letterSpacing: '0.1em',
                    }}>
                      {layer.label}
                    </span>
                    <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 9, color: '#A09488', marginLeft: 'auto' }}>
                      {layer.tables.length}
                    </span>
                  </div>

                  {/* Table cards */}
                  <div style={{ flex: 1, overflowY: 'auto', paddingRight: 4 }}>
                    {layer.tables.map((table) => (
                      <TableCard
                        key={table.name}
                        table={table}
                        layerColor={layer.color}
                        layerBorder={layer.border}
                        layerGlow={layer.glow}
                        layerLabel={layer.label}
                        isSelected={selectedTable === table.name}
                        onClick={() => setSelectedTable(selectedTable === table.name ? null : table.name)}
                        onLayerHover={() => setHoveredLayer(layer.id)}
                        onLayerLeave={() => setHoveredLayer(null)}
                      />
                    ))}
                  </div>
                </div>

                {/* Arrow connector between layers */}
                {lIdx < layers.length - 1 && (
                  <div style={{ width: 52, alignSelf: 'stretch', flexShrink: 0 }}>
                    <FlowArrow
                      color={layers[lIdx + 1].color}
                      boosted={
                        hoveredLayer === layer.id || hoveredLayer === layers[lIdx + 1].id
                      }
                    />
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </div>

      {/* Detail panel */}
      {selectedInfo && (
        <div
          className="gk-card animate-count-up"
          style={{
            flexShrink: 0, padding: '12px 16px',
            borderColor: selectedInfo.layerColor + '30',
            display: 'flex', alignItems: 'center', gap: 24,
            flexWrap: 'wrap',
          }}
        >
          <div>
            <div style={{ fontSize: 9, fontFamily: "'JetBrains Mono', monospace", color: '#A09488', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 3 }}>
              {selectedInfo.layerLabel} layer
            </div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontWeight: 600, fontSize: 14, color: selectedInfo.layerColor }}>
              {selectedInfo.name}
            </div>
          </div>
          <div style={{ flex: 1, fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#6B6256' }}>
            {selectedInfo.desc}
          </div>
          <JourneyTrace currentLayerLabel={selectedInfo.layerLabel} />
          {selectedInfo.rows != null && (
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 9, fontFamily: "'JetBrains Mono', monospace", color: '#A09488', marginBottom: 2 }}>ROW COUNT</div>
              <div style={{ fontFamily: 'Inter, sans-serif', fontWeight: 800, fontSize: 24, color: selectedInfo.layerColor, fontVariantNumeric: 'tabular-nums' }}>
                {selectedInfo.rows.toLocaleString()}
              </div>
            </div>
          )}
          <button
            onClick={() => setSelectedTable(null)}
            style={{ background: 'none', border: '1px solid #D9D1C4', borderRadius: 6, padding: '4px 10px', color: '#A09488', cursor: 'pointer', fontSize: 11, fontFamily: "'JetBrains Mono', monospace", transition: 'color 0.15s' }}
            onMouseEnter={(e) => { e.currentTarget.style.color = '#1C1A16' }}
            onMouseLeave={(e) => { e.currentTarget.style.color = '#A09488' }}
          >
            ✕
          </button>
        </div>
      )}

      {/* Bottom bar */}
      <div style={{
        flexShrink: 0, display: 'flex', alignItems: 'center', gap: 20, flexWrap: 'wrap',
        fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488',
        paddingTop: 6, borderTop: '1px solid #D9D1C4',
      }}>
        {layers.map((l) => (
          <span key={l.id} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: l.color }} />
            {l.label} ({l.tables.length})
          </span>
        ))}
        <span style={{ marginLeft: 'auto' }}>
          Click any table to inspect · Data Vault 2.0 · Star Schema
        </span>
      </div>
    </div>
  )
}
