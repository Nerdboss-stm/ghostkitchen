import { useState, useCallback } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  Handle,
  Position,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { motion, AnimatePresence } from 'framer-motion'
import { X, Key, Link, Columns } from 'lucide-react'
import { SCHEMA_NODES, SCHEMA_EDGES } from '../data/schemaData'

// ── Custom node: Fact ─────────────────────────────────────────────────────────
function FactNode({ data, selected }) {
  return (
    <div
      className={`rounded-xl border-2 px-4 py-3 cursor-pointer transition-all duration-200 ${
        selected
          ? 'border-[#D4866A] shadow-[0_4px_24px_rgba(212,134,106,0.3)]'
          : 'border-[#D4866A70] shadow-[0_2px_12px_rgba(212,134,106,0.1)]'
      }`}
      style={{ background: '#F3EFE8', minWidth: 180 }}
    >
      <Handle type="target" position={Position.Top} style={{ background: '#D4866A', border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Bottom} style={{ background: '#D4866A', border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Left} style={{ background: '#D4866A', border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Right} style={{ background: '#D4866A', border: 'none', width: 8, height: 8 }} />
      <div className="flex items-center justify-between mb-1">
        <span className="text-[8px] font-mono text-[#D4866A] font-bold tracking-widest uppercase bg-[#D4866A15] border border-[#D4866A30] px-2 py-0.5 rounded">
          FACT TABLE
        </span>
        <span className="text-[8px] text-[#A09488] font-mono">{data.rowCount}</span>
      </div>
      <div className="text-sm font-bold text-[#D4866A] font-mono">{data.label}</div>
    </div>
  )
}

// ── Custom node: Dim ──────────────────────────────────────────────────────────
function DimNode({ data, selected }) {
  return (
    <div
      className={`rounded-xl border cursor-pointer transition-all duration-200 px-3 py-2 ${
        selected
          ? 'border-[#BF953F] shadow-[0_4px_24px_rgba(191,149,63,0.3)]'
          : 'border-[#BF953F40] shadow-[0_2px_10px_rgba(191,149,63,0.1)]'
      }`}
      style={{ background: '#F3EFE8', minWidth: 140 }}
    >
      <Handle type="source" position={Position.Bottom} style={{ background: '#BF953F', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Top} style={{ background: '#BF953F', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Left} style={{ background: '#BF953F', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Right} style={{ background: '#BF953F', border: 'none', width: 6, height: 6 }} />
      <Handle type="target" position={Position.Bottom} id="t-b" style={{ background: '#BF953F', border: 'none', width: 6, height: 6, opacity: 0 }} />
      <Handle type="target" position={Position.Top} id="t-t" style={{ background: '#BF953F', border: 'none', width: 6, height: 6, opacity: 0 }} />
      <Handle type="target" position={Position.Left} id="t-l" style={{ background: '#BF953F', border: 'none', width: 6, height: 6, opacity: 0 }} />
      <Handle type="target" position={Position.Right} id="t-r" style={{ background: '#BF953F', border: 'none', width: 6, height: 6, opacity: 0 }} />
      <div className="flex items-center justify-between mb-0.5">
        <span className="text-[8px] font-mono text-[#BF953F] font-bold tracking-widest uppercase bg-[#BF953F10] border border-[#BF953F20] px-1.5 py-0.5 rounded">
          DIM
        </span>
        {data.scdType && (
          <span className="text-[8px] font-mono text-[#A09488] border border-[#D9D1C4] px-1.5 py-0.5 rounded">
            {data.scdType}
          </span>
        )}
      </div>
      <div className="text-xs font-bold text-[#BF953F] font-mono">{data.label}</div>
      <div className="text-[9px] text-[#A09488] mt-0.5">{data.rowCount} rows</div>
    </div>
  )
}

// ── Custom node: Silver Vault ─────────────────────────────────────────────────
function SilverNode({ data, selected }) {
  return (
    <div
      className={`rounded-xl border cursor-pointer transition-all duration-200 px-3 py-2 ${
        selected
          ? 'border-[#4A7C59] shadow-[0_4px_24px_rgba(74,124,89,0.3)]'
          : 'border-[#4A7C5940] shadow-[0_2px_10px_rgba(74,124,89,0.1)]'
      }`}
      style={{ background: '#F3EFE8', minWidth: 140 }}
    >
      <Handle type="source" position={Position.Bottom} style={{ background: '#4A7C59', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Top} style={{ background: '#4A7C59', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Left} style={{ background: '#4A7C59', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Right} style={{ background: '#4A7C59', border: 'none', width: 6, height: 6 }} />
      <Handle type="target" position={Position.Bottom} id="t-b" style={{ background: '#4A7C59', border: 'none', width: 6, height: 6, opacity: 0 }} />
      <Handle type="target" position={Position.Top} id="t-t" style={{ background: '#4A7C59', border: 'none', width: 6, height: 6, opacity: 0 }} />
      <Handle type="target" position={Position.Left} id="t-l" style={{ background: '#4A7C59', border: 'none', width: 6, height: 6, opacity: 0 }} />
      <Handle type="target" position={Position.Right} id="t-r" style={{ background: '#4A7C59', border: 'none', width: 6, height: 6, opacity: 0 }} />
      <div className="flex items-center justify-between mb-0.5">
        <span className="text-[8px] font-mono text-[#4A7C59] font-bold tracking-widest uppercase bg-[#4A7C5910] border border-[#4A7C5920] px-1.5 py-0.5 rounded">
          VAULT
        </span>
      </div>
      <div className="text-xs font-bold text-[#4A7C59] font-mono">{data.label}</div>
      <div className="text-[9px] text-[#A09488] mt-0.5">{data.rowCount} rows</div>
    </div>
  )
}

const nodeTypes = { factNode: FactNode, dimNode: DimNode, silverNode: SilverNode }

const LAYER_FILTERS = ['All', 'Facts', 'Dims', 'Vault']

function getLayerFilter(node, filter) {
  if (filter === 'All') return true
  if (filter === 'Facts') return node.type === 'factNode'
  if (filter === 'Dims') return node.type === 'dimNode'
  if (filter === 'Vault') return node.type === 'silverNode'
  return true
}

const edgeStyle = {
  stroke: '#BF953F',
  strokeWidth: 1.5,
  strokeDasharray: '6 3',
}

// ── Schema panel ──────────────────────────────────────────────────────────────
function SchemaPanel({ node, onClose }) {
  const d = node?.data
  if (!d) return null

  const isFact = node.type === 'factNode'
  const isSilver = node.type === 'silverNode'
  const color = isFact ? '#D4866A' : isSilver ? '#4A7C59' : '#BF953F'

  const roleIcon = (role) => {
    if (role === 'pk') return <Key size={10} style={{ color: '#D4866A' }} />
    if (role === 'fk') return <Link size={10} style={{ color: '#4A7C59' }} />
    return <Columns size={10} style={{ color: '#A09488' }} />
  }

  return (
    <motion.div
      initial={{ x: 380, opacity: 0 }}
      animate={{ x: 0, opacity: 1 }}
      exit={{ x: 380, opacity: 0 }}
      transition={{ type: 'spring', damping: 25, stiffness: 300 }}
      className="absolute right-0 top-0 bottom-0 w-80 gk-card rounded-l-2xl rounded-r-none border-r-0 z-20 flex flex-col overflow-hidden"
      style={{ borderColor: `${color}40` }}
    >
      {/* Header */}
      <div className="p-4 border-b" style={{ borderColor: '#D9D1C4', background: `${color}06` }}>
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <span
                className="text-[10px] font-mono font-bold tracking-widest uppercase px-2 py-0.5 rounded"
                style={{ color, background: `${color}12`, border: `1px solid ${color}28` }}
              >
                {isFact ? 'FACT TABLE' : isSilver ? 'VAULT' : 'DIMENSION'}
              </span>
              {d.scdType && (
                <span className="text-[10px] font-mono border px-2 py-0.5 rounded" style={{ color: '#A09488', borderColor: '#D9D1C4' }}>
                  {d.scdType}
                </span>
              )}
            </div>
            <h3 className="text-base font-bold font-mono" style={{ color }}>{d.label}</h3>
            <p className="text-xs mt-1 leading-relaxed" style={{ color: '#6B6256' }}>{d.description}</p>
          </div>
          <button
            onClick={onClose}
            className="ml-2 flex-shrink-0 transition-colors"
            style={{ color: '#A09488' }}
            onMouseEnter={(e) => { e.currentTarget.style.color = '#1C1A16' }}
            onMouseLeave={(e) => { e.currentTarget.style.color = '#A09488' }}
          >
            <X size={16} />
          </button>
        </div>
        {d.joinKey && (
          <div className="mt-2 text-[10px] font-mono px-3 py-1.5 rounded-lg" style={{ color: '#6B6256', background: '#EDE8DF' }}>
            🔗 {d.joinKey}
          </div>
        )}
      </div>

      {/* Columns */}
      <div className="flex-1 overflow-y-auto p-4">
        <div className="text-[10px] font-mono uppercase tracking-wider mb-3" style={{ color: '#A09488' }}>Columns</div>
        <div className="space-y-1 mb-5">
          {d.columns?.map((col) => (
            <div key={col.name} className="flex items-center gap-2 py-1.5 border-b" style={{ borderColor: '#D9D1C410' }}>
              {roleIcon(col.role)}
              <span className="text-xs font-mono flex-1" style={{
                color: col.role === 'pk' ? '#D4866A' : col.role === 'fk' ? '#4A7C59' : '#1C1A16',
              }}>
                {col.name}
              </span>
              <span className="text-[10px] font-mono" style={{ color: '#A09488' }}>{col.type}</span>
            </div>
          ))}
        </div>

        {d.sampleRows && (
          <>
            <div className="text-[10px] font-mono uppercase tracking-wider mb-2" style={{ color: '#A09488' }}>Sample Rows</div>
            <div className="rounded-lg overflow-hidden font-mono text-[10px]" style={{ border: '1px solid #D9D1C4' }}>
              {d.sampleRows.map((row, i) => (
                <div key={i} className="px-3 py-2 border-b last:border-0" style={{ borderColor: '#D9D1C4', background: i % 2 ? '#FAF8F4' : '#F3EFE8' }}>
                  {Object.entries(row).map(([k, v]) => (
                    <span key={k} className="mr-3">
                      <span style={{ color: '#A09488' }}>{k}:</span>
                      <span style={{ color: typeof v === 'boolean' ? (v ? '#4A7C59' : '#C0614A') : '#1C1A16' }}>
                        {' '}{String(v)}
                      </span>
                    </span>
                  ))}
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </motion.div>
  )
}

// ── Identity resolution visualizer ───────────────────────────────────────────
const RAW_RECORDS = [
  { platform: 'DoorDash', email: 'Sarah.J@Gmail.com', order_id: 'DD-8821', platform_color: '#C0614A' },
  { platform: 'Uber Eats', email: 'sarah.j@gmail.com', order_id: 'UE-4492', platform_color: '#BF953F' },
  { platform: 'OwnApp', email: 'SARAH.J@GMAIL.COM', order_id: 'OA-1103', platform_color: '#4A7C59' },
]

function IdentityVisualizer() {
  const mono = { fontFamily: "'JetBrains Mono', monospace" }
  return (
    <div style={{ padding: '20px 24px', height: '100%', overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 20, background: '#FAF8F4' }}>
      <div style={{ fontSize: 11, ...mono, color: '#6B6256' }}>
        Same customer, 3 platforms, 3 email formats → 1 unified identity
      </div>

      {/* Step 1: Raw records */}
      <div>
        <div style={{ fontSize: 9, ...mono, color: '#A09488', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 8 }}>① Raw ingestion (Bronze)</div>
        <div style={{ display: 'flex', gap: 8 }}>
          {RAW_RECORDS.map((r) => (
            <div key={r.platform} style={{
              flex: 1, padding: '8px 10px', borderRadius: 8,
              border: `1px solid ${r.platform_color}28`, background: `${r.platform_color}06`,
            }}>
              <div style={{ fontSize: 9, ...mono, color: r.platform_color, marginBottom: 4 }}>{r.platform}</div>
              <div style={{ fontSize: 9, ...mono, color: '#6B6256' }}>{r.email}</div>
              <div style={{ fontSize: 9, ...mono, color: '#A09488', marginTop: 2 }}>{r.order_id}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Arrow */}
      <div style={{ textAlign: 'center', fontSize: 12, color: '#A09488', ...mono }}>↓ normalize + MD5 hash</div>

      {/* Step 2: Normalized */}
      <div>
        <div style={{ fontSize: 9, ...mono, color: '#A09488', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 8 }}>② Normalized email hash (Silver)</div>
        <div style={{ display: 'flex', gap: 8 }}>
          {RAW_RECORDS.map((r) => (
            <div key={r.platform} style={{
              flex: 1, padding: '8px 10px', borderRadius: 8,
              border: '1px solid #D9D1C4', background: '#F3EFE8',
            }}>
              <div style={{ fontSize: 9, ...mono, color: '#A09488', marginBottom: 4 }}>{r.platform}</div>
              <div style={{ fontSize: 9, ...mono, color: '#4A7C59' }}>sarah.j@gmail.com</div>
              <div style={{ fontSize: 9, ...mono, color: '#A09488', marginTop: 2 }}>md5: a3f9…c21e</div>
            </div>
          ))}
        </div>
      </div>

      {/* Arrow */}
      <div style={{ textAlign: 'center', fontSize: 12, color: '#A09488', ...mono }}>↓ group by email_hash → assign customer_key</div>

      {/* Step 3: Resolved */}
      <div>
        <div style={{ fontSize: 9, ...mono, color: '#A09488', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 8 }}>③ Unified identity (Gold)</div>
        <div style={{
          padding: '14px 16px', borderRadius: 10,
          border: '1.5px solid #4A7C5940', background: 'rgba(74,124,89,0.05)',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        }}>
          <div>
            <div style={{ fontSize: 9, ...mono, color: '#4A7C59', marginBottom: 4 }}>dim_customer</div>
            <div style={{ fontSize: 11, ...mono, color: '#1C1A16', fontWeight: 700 }}>customer_key: ck_a3f9c21e</div>
            <div style={{ fontSize: 9, ...mono, color: '#A09488', marginTop: 4 }}>platforms: DoorDash · Uber Eats · OwnApp</div>
          </div>
          <div style={{
            padding: '4px 10px', borderRadius: 6,
            border: '1px solid rgba(74,124,89,0.3)', background: 'rgba(74,124,89,0.08)',
            fontSize: 9, ...mono, color: '#4A7C59',
          }}>
            match_confidence: 1.0
          </div>
        </div>
      </div>

      <div style={{ fontSize: 9, ...mono, color: '#A09488', borderTop: '1px solid #D9D1C4', paddingTop: 12 }}>
        PII rule: raw email stored in Silver only · MD5 hash propagated to Gold · GDPR-compliant delete via hub key
      </div>
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────
export default function SchemaExplorer() {
  const [selectedNode, setSelectedNode] = useState(null)
  const [layerFilter, setLayerFilter] = useState('All')
  const [view, setView] = useState('schema')

  const onNodeClick = useCallback((_, node) => {
    setSelectedNode((prev) => (prev?.id === node.id ? null : node))
  }, [])

  const filteredNodes = SCHEMA_NODES.filter((n) => getLayerFilter(n, layerFilter))
  const filteredNodeIds = new Set(filteredNodes.map((n) => n.id))
  const filteredEdges = SCHEMA_EDGES.filter((e) => filteredNodeIds.has(e.source) && filteredNodeIds.has(e.target))

  const styledEdges = filteredEdges.map((e) => ({
    ...e,
    style: edgeStyle,
    markerEnd: { type: 'arrowclosed', color: '#BF953F', width: 12, height: 12 },
  }))

  return (
    <div className="screen" style={{ padding: '12px 16px', gap: 10 }}>
      {/* Header row */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexShrink: 0 }}>
        <div>
          <div style={{
            display: 'inline-flex', alignItems: 'center', gap: 8,
            padding: '3px 10px', borderRadius: 20,
            border: '1px solid #D9D1C4', marginBottom: 4,
            fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#A09488',
          }}>
            Star Schema · Data Vault 2.0 · SCD Types 0/1/2
          </div>
          <h2 style={{
            fontFamily: "'Cormorant Garamond', Georgia, serif",
            fontStyle: 'italic',
            fontWeight: 600,
            fontSize: 24,
            color: '#1C1A16',
          }}>
            Schema Explorer
          </h2>
        </div>

        {/* Controls row */}
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          {['schema', 'identity'].map((v) => (
            <button
              key={v}
              onClick={() => setView(v)}
              style={{
                padding: '5px 12px', borderRadius: 6, fontSize: 11,
                fontFamily: 'Inter, sans-serif', fontWeight: 600,
                border: '1px solid',
                borderColor: view === v ? 'rgba(74,124,89,0.4)' : '#D9D1C4',
                background: view === v ? 'rgba(74,124,89,0.08)' : 'transparent',
                color: view === v ? '#4A7C59' : '#A09488',
                cursor: 'pointer', transition: 'all 0.15s',
              }}
            >
              {v === 'schema' ? 'Schema' : 'Identity Resolution'}
            </button>
          ))}
          {view === 'schema' && (
            <>
              <div style={{ width: 1, height: 16, background: '#D9D1C4' }} />
              {LAYER_FILTERS.map((f) => (
                <button
                  key={f}
                  onClick={() => setLayerFilter(f)}
                  style={{
                    padding: '5px 12px', borderRadius: 6, fontSize: 11,
                    fontFamily: 'Inter, sans-serif', fontWeight: 500,
                    border: '1px solid',
                    borderColor: layerFilter === f ? '#C4B99A' : '#D9D1C4',
                    background: layerFilter === f ? '#EDE8DF' : 'transparent',
                    color: layerFilter === f ? '#1C1A16' : '#A09488',
                    cursor: 'pointer', transition: 'all 0.15s',
                  }}
                >
                  {f}
                </button>
              ))}
            </>
          )}
        </div>
      </div>

      {/* Flow container */}
      <div style={{ flex: 1, position: 'relative', borderRadius: 12, overflow: 'hidden', border: '1px solid #D9D1C4', minHeight: 0, background: '#FAF8F4', boxShadow: '0 2px 12px rgba(28,26,22,0.05)' }}>
        {view === 'identity' && <IdentityVisualizer />}
        {view === 'schema' && (<>
        <ReactFlow
          nodes={filteredNodes.map((n) => ({
            ...n,
            selected: selectedNode?.id === n.id,
          }))}
          edges={styledEdges}
          nodeTypes={nodeTypes}
          onNodeClick={onNodeClick}
          fitView
          fitViewOptions={{ padding: 0.2 }}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable
          panOnDrag
          zoomOnScroll
        >
          <Background color="#D9D1C4" gap={28} size={1} />
          <Controls />
          <MiniMap
            nodeColor={(n) => (n.type === 'factNode' ? '#D4866A' : n.type === 'silverNode' ? '#4A7C59' : '#BF953F')}
            maskColor="rgba(250,248,244,0.7)"
          />
        </ReactFlow>

        {/* Schema panel */}
        <AnimatePresence>
          {selectedNode && (
            <SchemaPanel node={selectedNode} onClose={() => setSelectedNode(null)} />
          )}
        </AnimatePresence>

        {/* Hint */}
        {!selectedNode && (
          <div style={{
            position: 'absolute', bottom: 48, left: '50%', transform: 'translateX(-50%)',
            fontSize: 11, fontFamily: "'JetBrains Mono', monospace", color: '#A09488',
            background: '#F3EFE8', border: '1px solid #D9D1C4',
            padding: '5px 12px', borderRadius: 20, pointerEvents: 'none', whiteSpace: 'nowrap',
            boxShadow: '0 2px 8px rgba(28,26,22,0.06)',
          }}>
            Click any node to inspect schema
          </div>
        )}
        </>)}
      </div>

      {/* Legend */}
      <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 20, flexShrink: 0, paddingBottom: 4 }}>
        {[
          { color: '#D4866A', label: 'Fact Table (grain: 1 order)' },
          { color: '#BF953F', label: 'Dimension' },
          { color: '#4A7C59', label: 'Data Vault' },
        ].map(({ color, label }) => (
          <span key={label} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#A09488' }}>
            <span style={{ width: 10, height: 10, borderRadius: 2, border: `1px solid ${color}`, background: `${color}15` }} />
            {label}
          </span>
        ))}
        <span style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#A09488' }}>
          <span style={{ width: 12, height: 0, borderTop: '2px dashed #BF953F' }} />
          Foreign key join
        </span>
      </div>
    </div>
  )
}
