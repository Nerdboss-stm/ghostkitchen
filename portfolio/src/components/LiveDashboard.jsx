import { useState, useEffect, useCallback } from 'react'
import {
  LineChart, Line, BarChart, Bar, PieChart, Pie, Cell, ResponsiveContainer,
  CartesianGrid, XAxis, YAxis, Tooltip, ReferenceLine, Legend,
} from 'recharts'
import { RefreshCw, TrendingUp, TrendingDown, Clock, Zap } from 'lucide-react'
import { fetchAllDashboard } from '../lib/api'

const CHART_STYLE = {
  background: 'transparent',
  fontFamily: "'JetBrains Mono', monospace",
  fontSize: 11,
}
const AXIS_TICK = { fill: '#4a4a6a', fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }
const GRID_COLOR = '#1e1e3f'
const TOOLTIP_STYLE = {
  background: '#0d0d24',
  border: '1px solid #1e1e3f',
  borderRadius: 8,
  color: '#e8e8ff',
  fontSize: 11,
  fontFamily: "'JetBrains Mono', monospace",
}

function KpiCard({ label, value, sub, color = '#00d4ff', delta, icon: Icon }) {
  return (
    <div className="gk-card p-5" style={{ borderColor: `${color}30`, boxShadow: `0 0 20px ${color}05, inset 0 1px 0 ${color}10` }}>
      <div className="flex items-start justify-between mb-3">
        <span className="text-xs text-[#4a4a6a] font-mono uppercase tracking-wider">{label}</span>
        {Icon && <Icon size={14} style={{ color }} />}
      </div>
      <div className="text-3xl font-bold mb-1" style={{ color }}>
        {value ?? <span className="text-[#1e1e3f] animate-pulse">—</span>}
      </div>
      <div className="flex items-center gap-2">
        {delta !== undefined && (
          <span className={`text-xs flex items-center gap-0.5 ${delta >= 0 ? 'text-[#00ff88]' : 'text-[#ff4466]'}`}>
            {delta >= 0 ? <TrendingUp size={10} /> : <TrendingDown size={10} />}
            {Math.abs(delta)}%
          </span>
        )}
        {sub && <span className="text-xs text-[#4a4a6a]">{sub}</span>}
      </div>
    </div>
  )
}

function SectionTitle({ children }) {
  return (
    <h3 className="text-sm font-semibold text-[#8888aa] mb-4 font-mono uppercase tracking-wider flex items-center gap-2">
      <span className="w-1 h-4 rounded-full bg-[#00d4ff] inline-block" />
      {children}
    </h3>
  )
}

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center h-48 text-center">
      <div className="text-4xl mb-3">⚡</div>
      <p className="text-[#4a4a6a] text-sm">No data yet — run the pipeline first</p>
      <a href="#pipeline" className="text-[#00d4ff] text-xs mt-2 hover:underline">
        ↑ Go to Pipeline Orchestrator
      </a>
    </div>
  )
}

// ── Platform donut ────────────────────────────────────────────────────────────
const PLATFORM_COLORS = { uber_eats: '#00d4ff', doordash: '#9945ff', own_app: '#00ff88' }
const PLATFORM_LABELS = { uber_eats: 'Uber Eats', doordash: 'DoorDash', own_app: 'OwnApp' }

function PlatformDonut({ data }) {
  if (!data?.length) return <EmptyState />
  const total = data.reduce((s, d) => s + Number(d.order_count), 0)
  return (
    <div className="flex items-center gap-6">
      <ResponsiveContainer width={160} height={160}>
        <PieChart style={CHART_STYLE}>
          <Pie
            data={data}
            dataKey="order_count"
            nameKey="platform"
            cx="50%"
            cy="50%"
            innerRadius={45}
            outerRadius={70}
            strokeWidth={0}
          >
            {data.map((entry) => (
              <Cell key={entry.platform} fill={PLATFORM_COLORS[entry.platform] || '#4a4a6a'} />
            ))}
          </Pie>
          <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [v.toLocaleString(), 'orders']} />
        </PieChart>
      </ResponsiveContainer>
      <div className="flex-1 space-y-3">
        {data.map((d) => {
          const pct = total ? Math.round((d.order_count / total) * 100) : 0
          const color = PLATFORM_COLORS[d.platform] || '#4a4a6a'
          return (
            <div key={d.platform}>
              <div className="flex justify-between text-xs mb-1">
                <span className="font-mono" style={{ color }}>{PLATFORM_LABELS[d.platform] || d.platform}</span>
                <span className="text-[#4a4a6a]">{d.order_count?.toLocaleString()}</span>
              </div>
              <div className="h-1.5 rounded-full bg-[#1e1e3f]">
                <div className="h-full rounded-full transition-all duration-500" style={{ width: `${pct}%`, background: color }} />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Delivery zone bar ─────────────────────────────────────────────────────────
function DeliveryBar({ data }) {
  if (!data?.length) return <EmptyState />
  const sorted = [...data].sort((a, b) => Number(a.avg_duration_min) - Number(b.avg_duration_min)).slice(0, 10)
  const barColor = (v) => {
    const n = Number(v)
    if (n > 43) return '#ff4466'
    if (n > 35) return '#ffaa00'
    return '#00d4ff'
  }
  return (
    <ResponsiveContainer width="100%" height={220} style={CHART_STYLE}>
      <BarChart data={sorted} margin={{ top: 0, right: 10, left: -20, bottom: 0 }}>
        <CartesianGrid vertical={false} stroke={GRID_COLOR} />
        <XAxis dataKey="zone_id" tick={{ ...AXIS_TICK, fontSize: 9 }} tickLine={false} axisLine={false} />
        <YAxis tick={AXIS_TICK} tickLine={false} axisLine={false} />
        <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [`${v} min`, 'Avg duration']} />
        <ReferenceLine y={45} stroke="#ff4466" strokeDasharray="4 2" label={{ value: 'SLA 45m', fill: '#ff4466', fontSize: 9 }} />
        {sorted.map((entry, i) => (
          <Bar key={i} dataKey="avg_duration_min" fill={barColor(entry.avg_duration_min)} radius={[3, 3, 0, 0]} isAnimationActive />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

// ── Sensor anomaly stacked bar ────────────────────────────────────────────────
const SENSOR_COLORS = { temperature: '#ff4466', humidity: '#9945ff', co2: '#ffaa00', noise_db: '#00d4ff', fryer_timer: '#00ff88' }

function SensorBar({ data }) {
  if (!data?.length) return <EmptyState />
  const kitchens = [...new Set(data.map((d) => d.kitchen_id))].slice(0, 8)
  const types = [...new Set(data.map((d) => d.sensor_type))]
  const chartData = kitchens.map((kid) => {
    const entry = { kitchen_id: kid.replace('K-', '') }
    types.forEach((t) => {
      const row = data.find((d) => d.kitchen_id === kid && d.sensor_type === t)
      entry[t] = Number(row?.anomaly_count || 0)
    })
    return entry
  })
  return (
    <ResponsiveContainer width="100%" height={220} style={CHART_STYLE}>
      <BarChart data={chartData} margin={{ top: 0, right: 10, left: -20, bottom: 0 }}>
        <CartesianGrid vertical={false} stroke={GRID_COLOR} />
        <XAxis dataKey="kitchen_id" tick={AXIS_TICK} tickLine={false} axisLine={false} />
        <YAxis tick={AXIS_TICK} tickLine={false} axisLine={false} />
        <Tooltip contentStyle={TOOLTIP_STYLE} />
        {types.map((t) => (
          <Bar key={t} dataKey={t} stackId="a" fill={SENSOR_COLORS[t] || '#4a4a6a'} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

// ── Revenue line ──────────────────────────────────────────────────────────────
function RevenueLine({ data }) {
  if (!data?.length) return <EmptyState />
  const sorted = [...data].sort((a, b) => new Date(a.date) - new Date(b.date)).slice(-14)
  return (
    <ResponsiveContainer width="100%" height={220} style={CHART_STYLE}>
      <LineChart data={sorted} margin={{ top: 0, right: 10, left: -20, bottom: 0 }}>
        <CartesianGrid stroke={GRID_COLOR} />
        <XAxis
          dataKey="date"
          tick={{ ...AXIS_TICK, fontSize: 9 }}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v) => v?.slice(5)}
        />
        <YAxis tick={AXIS_TICK} tickLine={false} axisLine={false} tickFormatter={(v) => `$${(v / 100).toFixed(0)}`} />
        <Tooltip
          contentStyle={TOOLTIP_STYLE}
          formatter={(v, name) => [
            name === 'revenue_cents' ? `$${(v / 100).toFixed(2)}` : v,
            name === 'revenue_cents' ? 'Revenue' : 'Orders',
          ]}
        />
        <Line type="monotone" dataKey="revenue_cents" stroke="#00d4ff" strokeWidth={2} dot={false} />
        <Line type="monotone" dataKey="order_count" stroke="#9945ff" strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
        <Legend
          formatter={(v) => v === 'revenue_cents' ? 'Revenue (batch)' : 'Order count'}
          wrapperStyle={{ fontSize: 10, color: '#8888aa', fontFamily: "'JetBrains Mono'" }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}

// ── Top customers ─────────────────────────────────────────────────────────────
function TopCustomers({ data }) {
  if (!data?.length) return <EmptyState />
  const max = Math.max(...data.map((d) => Number(d.ltv_cents)))
  return (
    <div className="space-y-2 max-h-56 overflow-y-auto">
      {data.map((c, i) => {
        const pct = max ? (Number(c.ltv_cents) / max) * 100 : 0
        const multi = Number(c.platform_count) >= 2
        return (
          <div key={i} className={`flex items-center gap-3 py-2 px-3 rounded-lg ${multi ? 'border border-[#9945ff30] bg-[#9945ff08]' : ''}`}>
            <span className="text-xs text-[#4a4a6a] font-mono w-4">{i + 1}</span>
            <span className="text-xs font-mono text-[#e8e8ff] flex-1 truncate">
              {c.customer_hk?.slice(0, 10)}...
            </span>
            <div className="flex-1 h-1.5 bg-[#1e1e3f] rounded-full">
              <div
                className="h-full rounded-full transition-all duration-500"
                style={{ width: `${pct}%`, background: multi ? '#9945ff' : '#00d4ff' }}
              />
            </div>
            <span className="text-xs text-[#4a4a6a] font-mono w-16 text-right">
              ${(Number(c.ltv_cents) / 100).toFixed(0)}
            </span>
            {multi && (
              <span className="text-[8px] font-mono text-[#9945ff] border border-[#9945ff40] px-1.5 py-0.5 rounded flex-shrink-0">
                {c.platforms}
              </span>
            )}
          </div>
        )
      })}
    </div>
  )
}

// ── Kitchen capacity ──────────────────────────────────────────────────────────
function KitchenCapacity({ data }) {
  if (!data?.length) return <EmptyState />
  const sorted = [...data].sort((a, b) => Number(b.utilization_pct) - Number(a.utilization_pct)).slice(0, 10)
  const barColor = (v) => {
    if (Number(v) >= 80) return '#ff4466'
    if (Number(v) >= 60) return '#ffaa00'
    return '#00ff88'
  }
  return (
    <ResponsiveContainer width="100%" height={220} style={CHART_STYLE}>
      <BarChart data={sorted} layout="vertical" margin={{ top: 0, right: 30, left: 20, bottom: 0 }}>
        <CartesianGrid horizontal={false} stroke={GRID_COLOR} />
        <XAxis type="number" domain={[0, 100]} tick={AXIS_TICK} tickLine={false} axisLine={false} unit="%" />
        <YAxis type="category" dataKey="kitchen_id" tick={{ ...AXIS_TICK, fontSize: 9 }} tickLine={false} axisLine={false} width={60} />
        <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [`${v}%`, 'Utilization']} />
        <ReferenceLine x={100} stroke="#ff4466" strokeDasharray="4 2" />
        {sorted.map((entry, i) => (
          <Bar key={i} dataKey="utilization_pct" fill={barColor(entry.utilization_pct)} radius={[0, 3, 3, 0]} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

// ── Main dashboard ────────────────────────────────────────────────────────────
export default function LiveDashboard() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [error, setError] = useState(null)

  const refresh = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const result = await fetchAllDashboard()
      setData(result)
      setLastUpdated(new Date())
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    refresh()
    const t = setInterval(refresh, 30000)
    return () => clearInterval(t)
  }, [refresh])

  const kpis = data?.kpis
  const fmt = (cents) => cents != null ? `$${(cents / 100).toLocaleString('en-US', { minimumFractionDigits: 0 })}` : null

  return (
    <section id="dashboard" className="py-24 px-4 border-t border-[#1e1e3f]">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-center justify-between mb-10 gap-4">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <h2 className="text-4xl font-bold text-[#e8e8ff]">Live Dashboard</h2>
              <span className="flex items-center gap-1.5 text-xs font-mono text-[#00ff88] bg-[#00ff8815] border border-[#00ff8830] px-3 py-1 rounded-full">
                <span className="w-1.5 h-1.5 rounded-full bg-[#00ff88] animate-live-pulse" />
                LIVE
              </span>
            </div>
            <p className="text-[#4a4a6a] text-sm font-mono">
              Auto-refreshes every 30s ·{' '}
              {lastUpdated ? `Last updated ${lastUpdated.toLocaleTimeString()}` : 'Connecting...'}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs font-mono text-[#4a4a6a] border border-[#1e1e3f] px-3 py-1.5 rounded-lg">
              ⚡ Batch: daily · Speed: ~30s
            </span>
            <button
              onClick={refresh}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 rounded-lg border border-[#1e1e3f] text-[#8888aa] hover:text-[#e8e8ff] hover:border-[#8888aa] transition-colors text-sm"
            >
              <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
              Refresh
            </button>
          </div>
        </div>

        {error && (
          <div className="mb-6 p-4 rounded-xl border border-[#ff446640] bg-[#ff446610] text-[#ff4466] text-sm font-mono">
            ⚠ {error} — Run the pipeline to populate data.
          </div>
        )}

        {/* KPI row */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <KpiCard
            label="Total Revenue"
            value={fmt(kpis?.revenue_cents)}
            sub="from all orders"
            color="#00d4ff"
            icon={TrendingUp}
          />
          <KpiCard
            label="Orders"
            value={kpis?.order_count?.toLocaleString()}
            sub="normalised"
            color="#9945ff"
            icon={Zap}
          />
          <KpiCard
            label="Avg Delivery"
            value={kpis?.avg_delivery_min ? `${kpis.avg_delivery_min}m` : null}
            sub="SLA: 45 min"
            color={kpis?.avg_delivery_min > 43 ? '#ff4466' : '#00ff88'}
            icon={Clock}
          />
          <KpiCard
            label="SLA Breach"
            value={kpis?.sla_breach_pct != null ? `${kpis.sla_breach_pct}%` : null}
            sub="of deliveries"
            color={kpis?.sla_breach_pct > 20 ? '#ff4466' : '#ffaa00'}
          />
        </div>

        {/* Charts grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-5 mb-5">
          <div className="gk-card p-5">
            <SectionTitle>Revenue Trend (last 14 days)</SectionTitle>
            <RevenueLine data={data?.revenue} />
          </div>

          <div className="gk-card p-5">
            <SectionTitle>Orders by Platform</SectionTitle>
            <PlatformDonut data={data?.platforms} />
          </div>

          <div className="gk-card p-5">
            <SectionTitle>Delivery Time by Zone</SectionTitle>
            <DeliveryBar data={data?.zones} />
          </div>

          <div className="gk-card p-5">
            <SectionTitle>Sensor Anomalies by Kitchen</SectionTitle>
            <SensorBar data={data?.sensors} />
          </div>

          <div className="gk-card p-5">
            <SectionTitle>Top Customers by LTV</SectionTitle>
            <TopCustomers data={data?.customers} />
          </div>

          <div className="gk-card p-5">
            <SectionTitle>Kitchen Capacity Utilisation</SectionTitle>
            <KitchenCapacity data={data?.capacity} />
          </div>
        </div>

        {/* Footer */}
        <div className="section-divider mb-6" />
        <div className="flex flex-wrap justify-center gap-6 text-xs text-[#4a4a6a] font-mono">
          <span>Lambda Architecture · Batch + Speed Layer</span>
          <span>15 views · 11 batch + 4 Lambda UNION</span>
          <span>Data Vault 2.0 · SHA-256 identity resolution</span>
          <span>Built on Railway + Vercel</span>
        </div>
      </div>
    </section>
  )
}
