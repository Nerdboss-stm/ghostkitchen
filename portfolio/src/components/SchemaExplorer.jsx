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
          ? 'border-[#FFB547] shadow-[0_0_30px_rgba(255,181,71,0.5)]'
          : 'border-[#FFB54770] shadow-[0_0_15px_rgba(255,181,71,0.2)]'
      }`}
      style={{ background: '#0C1525', minWidth: 180 }}
    >
      <Handle type="target" position={Position.Top} style={{ background: '#FFB547', border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Bottom} style={{ background: '#FFB547', border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Left} style={{ background: '#FFB547', border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Right} style={{ background: '#FFB547', border: 'none', width: 8, height: 8 }} />
      <div className="flex items-center justify-between mb-1">
        <span className="text-[8px] font-mono text-[#FFB547] font-bold tracking-widest uppercase bg-[#FFB54715] border border-[#FFB54730] px-2 py-0.5 rounded">
          FACT TABLE
        </span>
        <span className="text-[8px] text-[#2D4060] font-mono">{data.rowCount}</span>
      </div>
      <div className="text-sm font-bold text-[#FFB547] font-mono">{data.label}</div>
    </div>
  )
}

// ── Custom node: Dim ──────────────────────────────────────────────────────────
function DimNode({ data, selected }) {
  return (
    <div
      className={`rounded-xl border cursor-pointer transition-all duration-200 px-3 py-2 ${
        selected
          ? 'border-[#00C2FF] shadow-[0_0_24px_rgba(0,194,255,0.5)]'
          : 'border-[#00C2FF40] shadow-[0_0_10px_rgba(0,194,255,0.1)]'
      }`}
      style={{ background: '#0C1525', minWidth: 140 }}
    >
      <Handle type="source" position={Position.Bottom} style={{ background: '#00C2FF', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Top} style={{ background: '#00C2FF', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Left} style={{ background: '#00C2FF', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Right} style={{ background: '#00C2FF', border: 'none', width: 6, height: 6 }} />
      <div className="flex items-center justify-between mb-0.5">
        <span className="text-[8px] font-mono text-[#00C2FF] font-bold tracking-widest uppercase bg-[#00C2FF10] border border-[#00C2FF20] px-1.5 py-0.5 rounded">
          DIM
        </span>
        {data.scdType && (
          <span className="text-[8px] font-mono text-[#2D4060] border border-[#142038] px-1.5 py-0.5 rounded">
            {data.scdType}
          </span>
        )}
      </div>
      <div className="text-xs font-bold text-[#00C2FF] font-mono">{data.label}</div>
      <div className="text-[9px] text-[#2D4060] mt-0.5">{data.rowCount} rows</div>
    </div>
  )
}

// ── Custom node: Silver Vault ─────────────────────────────────────────────────
function SilverNode({ data, selected }) {
  return (
    <div
      className={`rounded-xl border cursor-pointer transition-all duration-200 px-3 py-2 ${
        selected
          ? 'border-[#7C5CFC] shadow-[0_0_24px_rgba(124,92,252,0.5)]'
          : 'border-[#7C5CFC40] shadow-[0_0_10px_rgba(124,92,252,0.1)]'
      }`}
      style={{ background: '#0C1525', minWidth: 140 }}
    >
      <Handle type="source" position={Position.Bottom} style={{ background: '#7C5CFC', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Top} style={{ background: '#7C5CFC', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Left} style={{ background: '#7C5CFC', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Right} style={{ background: '#7C5CFC', border: 'none', width: 6, height: 6 }} />
      <div className="flex items-center justify-between mb-0.5">
        <span className="text-[8px] font-mono text-[#7C5CFC] font-bold tracking-widest uppercase bg-[#7C5CFC10] border border-[#7C5CFC20] px-1.5 py-0.5 rounded">
          VAULT
        </span>
      </div>
      <div className="text-xs font-bold text-[#7C5CFC] font-mono">{data.label}</div>
      <div className="text-[9px] text-[#2D4060] mt-0.5">{data.rowCount} rows</div>
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
  stroke: '#00C2FF',
  strokeWidth: 2,
  strokeDasharray: '6 3',
}

// ── Schema panel ──────────────────────────────────────────────────────────────
function SchemaPanel({ node, onClose }) {
  const d = node?.data
  if (!d) return null

  const isFact = node.type === 'factNode'
  const isSilver = node.type === 'silverNode'
  const color = isFact ? '#FFB547' : isSilver ? '#7C5CFC' : '#00C2FF'

  const roleIcon = (role) => {
    if (role === 'pk') return <Key size={10} className="text-[#FFB547]" />
    if (role === 'fk') return <Link size={10} className="text-[#7C5CFC]" />
    return <Columns size={10} className="text-[#2D4060]" />
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
      <div className="p-4 border-b border-[#142038]" style={{ background: `${color}08` }}>
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <span
                className="text-[10px] font-mono font-bold tracking-widest uppercase px-2 py-0.5 rounded"
                style={{ color, background: `${color}15`, border: `1px solid ${color}30` }}
              >
                {isFact ? 'FACT TABLE' : isSilver ? 'VAULT' : 'DIMENSION'}
              </span>
              {d.scdType && (
                <span className="text-[10px] font-mono text-[#2D4060] border border-[#142038] px-2 py-0.5 rounded">
                  {d.scdType}
                </span>
              )}
            </div>
            <h3 className="text-base font-bold font-mono" style={{ color }}>{d.label}</h3>
            <p className="text-xs text-[#6B82A8] mt-1 leading-relaxed">{d.description}</p>
          </div>
          <button
            onClick={onClose}
            className="text-[#2D4060] hover:text-[#D4E5FF] transition-colors ml-2 flex-shrink-0"
          >
            <X size={16} />
          </button>
        </div>
        {d.joinKey && (
          <div className="mt-2 text-[10px] font-mono text-[#2D4060] bg-[#142038] px-3 py-1.5 rounded-lg">
            🔗 {d.joinKey}
          </div>
        )}
      </div>

      {/* Columns */}
      <div className="flex-1 overflow-y-auto p-4">
        <div className="text-[10px] text-[#2D4060] font-mono uppercase tracking-wider mb-3">Columns</div>
        <div className="space-y-1 mb-5">
          {d.columns?.map((col) => (
            <div key={col.name} className="flex items-center gap-2 py-1.5 border-b border-[#14203810]">
              {roleIcon(col.role)}
              <span className={`text-xs font-mono flex-1 ${
                col.role === 'pk' ? 'text-[#FFB547]' :
                col.role === 'fk' ? 'text-[#7C5CFC]' : 'text-[#D4E5FF]'
              }`}>
                {col.name}
              </span>
              <span className="text-[10px] text-[#2D4060] font-mono">{col.type}</span>
            </div>
          ))}
        </div>

        {d.sampleRows && (
          <>
            <div className="text-[10px] text-[#2D4060] font-mono uppercase tracking-wider mb-2">Sample Rows</div>
            <div className="rounded-lg overflow-hidden border border-[#142038] font-mono text-[10px]">
              {d.sampleRows.map((row, i) => (
                <div key={i} className="px-3 py-2 border-b border-[#142038] last:border-0" style={{ background: i % 2 ? '#040912' : '#070E1A' }}>
                  {Object.entries(row).map(([k, v]) => (
                    <span key={k} className="mr-3">
                      <span className="text-[#2D4060]">{k}:</span>
                      <span className={typeof v === 'boolean' ? (v ? 'text-[#00E5A0]' : 'text-[#FF3D57]') : 'text-[#D4E5FF]'}>
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

// ── Main component ────────────────────────────────────────────────────────────
export default function SchemaExplorer() {
  const [selectedNode, setSelectedNode] = useState(null)
  const [layerFilter, setLayerFilter] = useState('All')

  const onNodeClick = useCallback((_, node) => {
    setSelectedNode((prev) => (prev?.id === node.id ? null : node))
  }, [])

  const filteredNodes = SCHEMA_NODES.filter((n) => getLayerFilter(n, layerFilter))
  const filteredNodeIds = new Set(filteredNodes.map((n) => n.id))
  const filteredEdges = SCHEMA_EDGES.filter((e) => filteredNodeIds.has(e.source) && filteredNodeIds.has(e.target))

  const styledEdges = filteredEdges.map((e) => ({
    ...e,
    style: edgeStyle,
    markerEnd: { type: 'arrowclosed', color: '#00C2FF', width: 12, height: 12 },
  }))

  return (
    <div className="screen" style={{ padding: '12px 16px', gap: 10 }}>
      {/* Header row */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexShrink: 0 }}>
        <div>
          <div style={{
            display: 'inline-flex', alignItems: 'center', gap: 8,
            padding: '3px 10px', borderRadius: 20,
            border: '1px solid #142038', marginBottom: 4,
            fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#2D4060',
          }}>
            Star Schema · Data Vault 2.0 · SCD Types 0/1/2
          </div>
          <h2 style={{ fontFamily: 'Syne, sans-serif', fontWeight: 800, fontSize: 22, color: '#D4E5FF' }}>
            Schema Explorer
          </h2>
        </div>

        {/* Layer filter */}
        <div style={{ display: 'flex', gap: 6 }}>
          {LAYER_FILTERS.map((f) => (
            <button
              key={f}
              onClick={() => setLayerFilter(f)}
              style={{
                padding: '5px 12px', borderRadius: 6, fontSize: 11,
                fontFamily: 'Syne, sans-serif', fontWeight: 500,
                border: '1px solid',
                borderColor: layerFilter === f ? '#1E3254' : '#142038',
                background: layerFilter === f ? '#0C1525' : 'transparent',
                color: layerFilter === f ? '#D4E5FF' : '#2D4060',
                cursor: 'pointer', transition: 'all 0.15s',
              }}
            >
              {f}
            </button>
          ))}
        </div>
      </div>

      {/* Flow container */}
      <div style={{ flex: 1, position: 'relative', borderRadius: 12, overflow: 'hidden', border: '1px solid #142038', minHeight: 0 }}>
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
          <Background color="#142038" gap={28} size={1} />
          <Controls />
          <MiniMap
            nodeColor={(n) => (n.type === 'factNode' ? '#FFB547' : n.type === 'silverNode' ? '#7C5CFC' : '#00C2FF')}
            maskColor="rgba(4,9,18,0.7)"
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
            fontSize: 11, fontFamily: "'JetBrains Mono', monospace", color: '#2D4060',
            background: '#070E1A', border: '1px solid #142038',
            padding: '5px 12px', borderRadius: 20, pointerEvents: 'none', whiteSpace: 'nowrap',
          }}>
            Click any node to inspect schema
          </div>
        )}
      </div>

      {/* Legend */}
      <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 20, flexShrink: 0, paddingBottom: 4 }}>
        {[
          { color: '#FFB547', label: 'Fact Table (grain: 1 order)', type: 'box' },
          { color: '#00C2FF', label: 'Dimension', type: 'box' },
          { color: '#7C5CFC', label: 'Data Vault', type: 'box' },
        ].map(({ color, label }) => (
          <span key={label} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#2D4060' }}>
            <span style={{ width: 10, height: 10, borderRadius: 2, border: `1px solid ${color}`, background: `${color}15` }} />
            {label}
          </span>
        ))}
        <span style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#2D4060' }}>
          <span style={{ width: 12, height: 0, borderTop: '2px dashed #00C2FF' }} />
          Foreign key join
        </span>
      </div>
    </div>
  )
}
