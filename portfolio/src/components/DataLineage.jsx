import { useState, useEffect } from 'react'
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

function FlowArrow({ color }) {
  return (
    <div style={{
      width: 48, flexShrink: 0, display: 'flex',
      flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
      position: 'relative',
    }}>
      <svg width="48" height="100%" style={{ position: 'absolute', top: 0, left: 0, height: '100%' }}>
        <defs>
          <marker id={`arrow-${color.replace('#', '')}`} markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
            <path d="M0,0 L0,6 L8,3 z" fill={color} />
          </marker>
        </defs>
        <line
          x1="0" y1="50%" x2="38" y2="50%"
          stroke={color}
          strokeWidth="1.5"
          strokeDasharray="6 3"
          markerEnd={`url(#arrow-${color.replace('#', '')})`}
          className="animate-flow-line"
          style={{ strokeDashoffset: 0, animation: 'flowDash 1.5s linear infinite' }}
        />
      </svg>
      {/* Animated dot */}
      <div style={{
        position: 'absolute', width: 6, height: 6, borderRadius: '50%',
        background: color, opacity: 0.6,
        animation: 'dataFlow 2s ease-in-out infinite',
      }} />
    </div>
  )
}

function TableCard({ table, layerColor, layerBorder, layerGlow, isSelected, onClick }) {
  const [hovered, setHovered] = useState(false)

  return (
    <div
      onClick={onClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        padding: '10px 12px', borderRadius: 8, cursor: 'pointer',
        border: `1px solid`,
        borderColor: isSelected ? layerColor : hovered ? layerBorder : '#D9D1C4',
        background: isSelected ? layerGlow : hovered ? 'rgba(28,26,22,0.02)' : '#F3EFE8',
        transition: 'all 0.15s',
        boxShadow: isSelected ? `0 4px 16px ${layerGlow}` : hovered ? '0 2px 8px rgba(28,26,22,0.06)' : 'none',
        marginBottom: 6,
      }}
    >
      <div style={{
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: 11, fontWeight: 500,
        color: isSelected ? layerColor : '#1C1A16',
        marginBottom: 2, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
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

export default function DataLineage() {
  const [lineageData, setLineageData] = useState(null)
  const [selectedTable, setSelectedTable] = useState(null)
  const [lastRun, setLastRun] = useState(null)

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
          {layers.map((layer, lIdx) => (
            <div key={layer.id} style={{ display: 'flex', alignItems: 'stretch' }}>
              {/* Layer column */}
              <div style={{ width: 196, display: 'flex', flexDirection: 'column' }}>
                {/* Layer header */}
                <div style={{
                  padding: '6px 10px', marginBottom: 8, borderRadius: 6,
                  border: `1px solid ${layer.border}`,
                  background: layer.glow,
                  display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0,
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
                      isSelected={selectedTable === table.name}
                      onClick={() => setSelectedTable(selectedTable === table.name ? null : table.name)}
                    />
                  ))}
                </div>
              </div>

              {/* Arrow connector between layers */}
              {lIdx < layers.length - 1 && (
                <div style={{ width: 48, alignSelf: 'center', flexShrink: 0, padding: '0 4px' }}>
                  <FlowArrow color={layers[lIdx + 1].color} />
                </div>
              )}
            </div>
          ))}
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
          {selectedInfo.rows != null && (
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 9, fontFamily: "'JetBrains Mono', monospace", color: '#A09488', marginBottom: 2 }}>ROW COUNT</div>
              <div style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: 'italic', fontWeight: 600, fontSize: 24, color: selectedInfo.layerColor }}>
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
