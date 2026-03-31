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
          ? 'border-[#ffaa00] shadow-[0_0_30px_rgba(255,170,0,0.5)]'
          : 'border-[#ffaa0070] shadow-[0_0_15px_rgba(255,170,0,0.2)]'
      }`}
      style={{ background: '#0d0d24', minWidth: 180 }}
    >
      <Handle type="target" position={Position.Top} style={{ background: '#ffaa00', border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Bottom} style={{ background: '#ffaa00', border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Left} style={{ background: '#ffaa00', border: 'none', width: 8, height: 8 }} />
      <Handle type="target" position={Position.Right} style={{ background: '#ffaa00', border: 'none', width: 8, height: 8 }} />
      <div className="flex items-center justify-between mb-1">
        <span className="text-[8px] font-mono text-[#ffaa00] font-bold tracking-widest uppercase bg-[#ffaa0015] border border-[#ffaa0030] px-2 py-0.5 rounded">
          FACT TABLE
        </span>
        <span className="text-[8px] text-[#4a4a6a] font-mono">{data.rowCount}</span>
      </div>
      <div className="text-sm font-bold text-[#ffaa00] font-mono">{data.label}</div>
    </div>
  )
}

// ── Custom node: Dim ──────────────────────────────────────────────────────────
function DimNode({ data, selected }) {
  return (
    <div
      className={`rounded-xl border cursor-pointer transition-all duration-200 px-3 py-2 ${
        selected
          ? 'border-[#00d4ff] shadow-[0_0_20px_rgba(0,212,255,0.5)]'
          : 'border-[#00d4ff40] shadow-[0_0_10px_rgba(0,212,255,0.1)]'
      }`}
      style={{ background: '#0d0d24', minWidth: 140 }}
    >
      <Handle type="source" position={Position.Bottom} style={{ background: '#00d4ff', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Top} style={{ background: '#00d4ff', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Left} style={{ background: '#00d4ff', border: 'none', width: 6, height: 6 }} />
      <Handle type="source" position={Position.Right} style={{ background: '#00d4ff', border: 'none', width: 6, height: 6 }} />
      <div className="flex items-center justify-between mb-0.5">
        <span className="text-[8px] font-mono text-[#00d4ff] font-bold tracking-widest uppercase bg-[#00d4ff10] border border-[#00d4ff20] px-1.5 py-0.5 rounded">
          DIM
        </span>
        {data.scdType && (
          <span className="text-[8px] font-mono text-[#4a4a6a] border border-[#1e1e3f] px-1.5 py-0.5 rounded">
            {data.scdType}
          </span>
        )}
      </div>
      <div className="text-xs font-bold text-[#00d4ff] font-mono">{data.label}</div>
      <div className="text-[9px] text-[#4a4a6a] mt-0.5">{data.rowCount} rows</div>
    </div>
  )
}

const nodeTypes = { factNode: FactNode, dimNode: DimNode }

const edgeStyle = {
  stroke: '#00d4ff',
  strokeWidth: 2,
  strokeDasharray: '6 3',
}

// ── Schema panel ──────────────────────────────────────────────────────────────
function SchemaPanel({ node, onClose }) {
  const d = node?.data
  if (!d) return null

  const isFact = node.type === 'factNode'
  const color = isFact ? '#ffaa00' : '#00d4ff'

  const roleIcon = (role) => {
    if (role === 'pk') return <Key size={10} className="text-[#ffaa00]" />
    if (role === 'fk') return <Link size={10} className="text-[#9945ff]" />
    return <Columns size={10} className="text-[#4a4a6a]" />
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
      <div className="p-4 border-b border-[#1e1e3f]" style={{ background: `${color}08` }}>
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <span
                className="text-[10px] font-mono font-bold tracking-widest uppercase px-2 py-0.5 rounded"
                style={{ color, background: `${color}15`, border: `1px solid ${color}30` }}
              >
                {isFact ? 'FACT TABLE' : 'DIMENSION'}
              </span>
              {d.scdType && (
                <span className="text-[10px] font-mono text-[#4a4a6a] border border-[#1e1e3f] px-2 py-0.5 rounded">
                  {d.scdType}
                </span>
              )}
            </div>
            <h3 className="text-base font-bold font-mono" style={{ color }}>{d.label}</h3>
            <p className="text-xs text-[#8888aa] mt-1 leading-relaxed">{d.description}</p>
          </div>
          <button
            onClick={onClose}
            className="text-[#4a4a6a] hover:text-[#e8e8ff] transition-colors ml-2 flex-shrink-0"
          >
            <X size={16} />
          </button>
        </div>
        {d.joinKey && (
          <div className="mt-2 text-[10px] font-mono text-[#4a4a6a] bg-[#1e1e3f] px-3 py-1.5 rounded-lg">
            🔗 {d.joinKey}
          </div>
        )}
      </div>

      {/* Columns */}
      <div className="flex-1 overflow-y-auto p-4">
        <div className="text-[10px] text-[#4a4a6a] font-mono uppercase tracking-wider mb-3">Columns</div>
        <div className="space-y-1 mb-5">
          {d.columns?.map((col) => (
            <div key={col.name} className="flex items-center gap-2 py-1.5 border-b border-[#1e1e3f10]">
              {roleIcon(col.role)}
              <span className={`text-xs font-mono flex-1 ${
                col.role === 'pk' ? 'text-[#ffaa00]' :
                col.role === 'fk' ? 'text-[#9945ff]' : 'text-[#e8e8ff]'
              }`}>
                {col.name}
              </span>
              <span className="text-[10px] text-[#4a4a6a] font-mono">{col.type}</span>
            </div>
          ))}
        </div>

        {/* Sample rows */}
        {d.sampleRows && (
          <>
            <div className="text-[10px] text-[#4a4a6a] font-mono uppercase tracking-wider mb-2">Sample Rows</div>
            <div className="rounded-lg overflow-hidden border border-[#1e1e3f] font-mono text-[10px]">
              {d.sampleRows.map((row, i) => (
                <div key={i} className="px-3 py-2 border-b border-[#1e1e3f] last:border-0" style={{ background: i % 2 ? '#05050f' : '#0a0a1f' }}>
                  {Object.entries(row).map(([k, v]) => (
                    <span key={k} className="mr-3">
                      <span className="text-[#4a4a6a]">{k}:</span>
                      <span className={typeof v === 'boolean' ? (v ? 'text-[#00ff88]' : 'text-[#ff4466]') : 'text-[#e8e8ff]'}>
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

  const onNodeClick = useCallback((_, node) => {
    setSelectedNode((prev) => (prev?.id === node.id ? null : node))
  }, [])

  const styledEdges = SCHEMA_EDGES.map((e) => ({
    ...e,
    style: edgeStyle,
    markerEnd: { type: 'arrowclosed', color: '#00d4ff', width: 12, height: 12 },
  }))

  return (
    <section id="schema" className="py-24 px-4">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="text-center mb-12">
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full border border-[#1e1e3f] text-xs font-mono text-[#4a4a6a] mb-4">
            Star Schema · Data Vault 2.0 · SCD Types 0/1/2
          </div>
          <h2 className="text-4xl font-bold text-[#e8e8ff] mb-3">Schema Explorer</h2>
          <p className="text-[#8888aa] max-w-xl mx-auto">
            Interactive star schema. Click any table to inspect columns, SCD type, join keys, and sample rows.
          </p>
        </div>

        {/* Flow container */}
        <div className="relative rounded-2xl overflow-hidden border border-[#1e1e3f]" style={{ height: 560 }}>
          <ReactFlow
            nodes={SCHEMA_NODES.map((n) => ({
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
            <Background color="#1e1e3f" gap={28} size={1} />
            <Controls />
            <MiniMap
              nodeColor={(n) => (n.type === 'factNode' ? '#ffaa00' : '#00d4ff')}
              maskColor="#07071a99"
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
            <div className="absolute bottom-14 left-1/2 -translate-x-1/2 text-xs text-[#4a4a6a] font-mono bg-[#07071a] border border-[#1e1e3f] px-3 py-1.5 rounded-full pointer-events-none">
              Click any node to inspect schema
            </div>
          )}
        </div>

        {/* Legend */}
        <div className="flex flex-wrap justify-center gap-6 mt-6 text-xs font-mono text-[#4a4a6a]">
          <span className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-sm border border-[#ffaa00] bg-[#ffaa0015]" />
            Fact Table (grain: 1 order)
          </span>
          <span className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-sm border border-[#00d4ff] bg-[#00d4ff10]" />
            Dimension
          </span>
          <span className="flex items-center gap-2">
            <span className="w-3 h-0.5 bg-[#00d4ff]" style={{ borderTop: '2px dashed #00d4ff' }} />
            Foreign key join
          </span>
          <span className="flex items-center gap-2">
            <span className="border border-[#1e1e3f] px-1.5 py-0.5 rounded text-[10px]">SCD0</span>
            Static
          </span>
          <span className="flex items-center gap-2">
            <span className="border border-[#1e1e3f] px-1.5 py-0.5 rounded text-[10px]">SCD2</span>
            History tracked
          </span>
        </div>
      </div>
    </section>
  )
}
