import { useState, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
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
const AXIS_TICK = { fill: '#2D4060', fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }
const GRID_COLOR = '#142038'
const TOOLTIP_STYLE = {
  background: '#0C1525',
  border: '1px solid #1E3254',
  borderRadius: 8,
  color: '#D4E5FF',
  fontSize: 11,
  fontFamily: "'JetBrains Mono', monospace",
}

function KpiCard({ label, value, sub, color = '#00C2FF', delta, icon: Icon }) {
  return (
    <div className="gk-card" style={{ padding: 20, borderColor: `${color}30`, boxShadow: `0 0 24px ${color}05, inset 0 1px 0 ${color}10` }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 10 }}>
        <span style={{ fontSize: 10, color: '#2D4060', fontFamily: "'JetBrains Mono', monospace", textTransform: 'uppercase', letterSpacing: '0.08em' }}>{label}</span>
        {Icon && <Icon size={13} style={{ color }} />}
      </div>
      <div style={{ fontFamily: 'Inter, sans-serif', fontSize: 30, fontWeight: 800, marginBottom: 4, color }}>
        {value ?? <span style={{ color: '#142038' }}>—</span>}
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        {delta !== undefined && (
          <span style={{ fontSize: 11, display: 'flex', alignItems: 'center', gap: 3, color: delta >= 0 ? '#00E5A0' : '#FF3D57' }}>
            {delta >= 0 ? <TrendingUp size={10} /> : <TrendingDown size={10} />}
            {Math.abs(delta)}%
          </span>
        )}
        {sub && <span style={{ fontSize: 11, color: '#2D4060' }}>{sub}</span>}
      </div>
    </div>
  )
}

function SectionTitle({ children }) {
  return (
    <h3 style={{
      fontFamily: 'Inter, sans-serif', fontWeight: 600, fontSize: 12,
      color: '#6B82A8', marginBottom: 14, textTransform: 'uppercase',
      letterSpacing: '0.06em', display: 'flex', alignItems: 'center', gap: 8,
    }}>
      <span style={{ width: 3, height: 14, borderRadius: 2, background: '#00C2FF', display: 'inline-block' }} />
      {children}
    </h3>
  )
}

function EmptyState() {
  const navigate = useNavigate()
  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: 180, textAlign: 'center' }}>
      <div style={{ fontSize: 32, marginBottom: 10 }}>⚡</div>
      <p style={{ color: '#2D4060', fontSize: 12, fontFamily: "'JetBrains Mono', monospace", textAlign: 'center' }}>Nothing here yet — hit Run Pipeline,<br />takes ~60s, writes real rows to PostgreSQL.</p>
      <button
        onClick={() => navigate('/')}
        style={{ color: '#0EA5E9', fontSize: 11, marginTop: 10, background: 'none', border: 'none', cursor: 'pointer', textDecoration: 'underline', fontFamily: "'JetBrains Mono', monospace" }}
      >
        → Run Pipeline
      </button>
    </div>
  )
}

const PLATFORM_COLORS = { uber_eats: '#00C2FF', doordash: '#7C5CFC', own_app: '#00E5A0' }
const PLATFORM_LABELS = { uber_eats: 'Uber Eats', doordash: 'DoorDash', own_app: 'OwnApp' }

function PlatformDonut({ data }) {
  if (!data?.length) return <EmptyState />
  const total = data.reduce((s, d) => s + Number(d.order_count), 0)
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
      <ResponsiveContainer width={150} height={150}>
        <PieChart style={CHART_STYLE}>
          <Pie data={data} dataKey="order_count" nameKey="platform" cx="50%" cy="50%" innerRadius={42} outerRadius={65} strokeWidth={0}>
            {data.map((entry) => (
              <Cell key={entry.platform} fill={PLATFORM_COLORS[entry.platform] || '#2D4060'} />
            ))}
          </Pie>
          <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [v.toLocaleString(), 'orders']} />
        </PieChart>
      </ResponsiveContainer>
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 10 }}>
        {data.map((d) => {
          const pct = total ? Math.round((d.order_count / total) * 100) : 0
          const color = PLATFORM_COLORS[d.platform] || '#2D4060'
          return (
            <div key={d.platform}>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, marginBottom: 4 }}>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", color }}>{PLATFORM_LABELS[d.platform] || d.platform}</span>
                <span style={{ color: '#2D4060', fontFamily: "'JetBrains Mono', monospace" }}>{d.order_count?.toLocaleString()}</span>
              </div>
              <div style={{ height: 5, borderRadius: 3, background: '#142038' }}>
                <div style={{ height: '100%', borderRadius: 3, transition: 'width 0.5s', width: `${pct}%`, background: color }} />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function DeliveryBar({ data }) {
  if (!data?.length) return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: 180, gap: 8 }}>
      <div style={{ fontSize: 28 }}>🗺️</div>
      <p style={{ color: '#2D4060', fontSize: 11, fontFamily: "'JetBrains Mono', monospace", textAlign: 'center' }}>
        Run the pipeline to populate<br />delivery trip data
      </p>
    </div>
  )
  const sorted = [...data].sort((a, b) => Number(a.avg_duration_min) - Number(b.avg_duration_min)).slice(0, 10)
  const barColor = (v) => {
    const n = Number(v)
    if (n > 43) return '#FF3D57'
    if (n > 35) return '#FFB547'
    return '#00C2FF'
  }
  return (
    <ResponsiveContainer width="100%" height={220} style={CHART_STYLE}>
      <BarChart data={sorted} margin={{ top: 4, right: 10, left: -20, bottom: 0 }}>
        <CartesianGrid vertical={false} stroke={GRID_COLOR} />
        <XAxis dataKey="zone_id" tick={{ ...AXIS_TICK, fontSize: 8 }} tickLine={false} axisLine={false} />
        <YAxis tick={AXIS_TICK} tickLine={false} axisLine={false} unit="m" />
        <Tooltip
          contentStyle={TOOLTIP_STYLE}
          formatter={(v, _, props) => [
            `${v} min · ${props.payload.trip_count} trip${props.payload.trip_count !== 1 ? 's' : ''}`,
            props.payload.zone_type || 'Zone',
          ]}
        />
        <ReferenceLine y={45} stroke="#FF3D57" strokeDasharray="4 2" label={{ value: 'SLA', fill: '#FF3D57', fontSize: 9, position: 'right' }} />
        {sorted.map((entry, i) => (
          <Bar key={i} dataKey="avg_duration_min" fill={barColor(entry.avg_duration_min)} radius={[3, 3, 0, 0]} isAnimationActive />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

const SENSOR_COLORS = { temperature: '#FF3D57', humidity: '#7C5CFC', co2: '#FFB547', noise_db: '#00C2FF', fryer_timer: '#00E5A0' }

function SensorBar({ data }) {
  if (!data?.length) return <EmptyState />
  const kitchens = [...new Set(data.map((d) => d.kitchen_id))].slice(0, 8)
  const types = [...new Set(data.map((d) => d.sensor_type))]
  const chartData = kitchens.map((kid) => {
    const entry = { kitchen_id: kid.replace('K-', '') }
    types.forEach((t) => {
      const row = data.find((d) => d.kitchen_id === kid && d.sensor_type === t)
      entry[t] = Number(row?.reading_count || 0)
      entry[`${t}_anomalies`] = Number(row?.anomaly_count || 0)
    })
    return entry
  })
  return (
    <ResponsiveContainer width="100%" height={220} style={CHART_STYLE}>
      <BarChart data={chartData} margin={{ top: 0, right: 10, left: -20, bottom: 0 }}>
        <CartesianGrid vertical={false} stroke={GRID_COLOR} />
        <XAxis dataKey="kitchen_id" tick={AXIS_TICK} tickLine={false} axisLine={false} />
        <YAxis tick={AXIS_TICK} tickLine={false} axisLine={false} />
        <Tooltip
          contentStyle={TOOLTIP_STYLE}
          formatter={(v, name, props) => {
            const anomalies = props.payload[`${name}_anomalies`] || 0
            return [`${v} readings${anomalies ? ` (${anomalies} anomalies)` : ''}`, name]
          }}
        />
        {types.map((t) => (
          <Bar key={t} dataKey={t} stackId="a" fill={SENSOR_COLORS[t] || '#2D4060'} name={t} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

function RevenueLine({ data }) {
  if (!data?.length) return <EmptyState />
  const sorted = [...data].sort((a, b) => new Date(a.date) - new Date(b.date)).slice(-14)
  return (
    <ResponsiveContainer width="100%" height={220} style={CHART_STYLE}>
      <LineChart data={sorted} margin={{ top: 0, right: 10, left: -20, bottom: 0 }}>
        <CartesianGrid stroke={GRID_COLOR} />
        <XAxis dataKey="date" tick={{ ...AXIS_TICK, fontSize: 9 }} tickLine={false} axisLine={false} tickFormatter={(v) => v?.slice(5)} />
        <YAxis tick={AXIS_TICK} tickLine={false} axisLine={false} tickFormatter={(v) => `$${(v / 100).toFixed(0)}`} />
        <Tooltip
          contentStyle={TOOLTIP_STYLE}
          formatter={(v, name) => [
            name === 'revenue_cents' ? `$${(v / 100).toFixed(2)}` : v,
            name === 'revenue_cents' ? 'Revenue' : 'Orders',
          ]}
        />
        <Line type="monotone" dataKey="revenue_cents" stroke="#00C2FF" strokeWidth={2} dot={false} />
        <Line type="monotone" dataKey="order_count" stroke="#7C5CFC" strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
        <Legend formatter={(v) => v === 'revenue_cents' ? 'Revenue (batch)' : 'Order count'} wrapperStyle={{ fontSize: 10, color: '#6B82A8', fontFamily: "'JetBrains Mono'" }} />
      </LineChart>
    </ResponsiveContainer>
  )
}

function TopCustomers({ data }) {
  if (!data?.length) return <EmptyState />
  const max = Math.max(...data.map((d) => Number(d.ltv_cents)))
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 5, maxHeight: 220, overflowY: 'auto' }}>
      {data.map((c, i) => {
        const pct = max ? (Number(c.ltv_cents) / max) * 100 : 0
        const multi = Number(c.platform_count) >= 2
        const orders = Number(c.order_count)
        const barColor = orders >= 5 ? '#00E5A0' : orders >= 3 ? '#00C2FF' : multi ? '#7C5CFC' : '#1E3254'
        return (
          <div key={i} style={{
            display: 'flex', alignItems: 'center', gap: 8, padding: '7px 10px', borderRadius: 6,
            border: `1px solid ${multi ? 'rgba(124,92,252,0.25)' : '#142038'}`,
            background: multi ? 'rgba(124,92,252,0.05)' : i % 2 === 0 ? '#070E1A' : 'transparent',
          }}>
            <span style={{ fontSize: 10, color: '#2D4060', fontFamily: "'JetBrains Mono', monospace", width: 14, flexShrink: 0 }}>{i + 1}</span>
            <span style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#6B82A8', width: 80, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flexShrink: 0 }}>
              {c.customer_hk?.slice(0, 8)}…
            </span>
            <div style={{ flex: 1, height: 6, background: '#142038', borderRadius: 3, overflow: 'hidden' }}>
              <div style={{ height: '100%', borderRadius: 3, transition: 'width 0.6s ease', width: `${pct}%`, background: barColor }} />
            </div>
            <span style={{ fontSize: 11, fontFamily: "'JetBrains Mono', monospace", color: '#D4E5FF', width: 52, textAlign: 'right', flexShrink: 0, fontWeight: 600 }}>
              ${(Number(c.ltv_cents) / 100).toFixed(0)}
            </span>
            <span style={{ fontSize: 9, fontFamily: "'JetBrains Mono', monospace", color: orders >= 3 ? '#00E5A0' : '#2D4060', border: `1px solid ${orders >= 3 ? 'rgba(0,229,160,0.3)' : '#142038'}`, padding: '1px 5px', borderRadius: 3, flexShrink: 0, minWidth: 28, textAlign: 'center' }}>
              {orders}x
            </span>
          </div>
        )
      })}
    </div>
  )
}

function KitchenCapacity({ data }) {
  if (!data?.length) return <EmptyState />
  const sorted = [...data].sort((a, b) => Number(b.utilization_pct) - Number(a.utilization_pct)).slice(0, 10)
  const barColor = (v) => {
    if (Number(v) >= 80) return '#FF3D57'
    if (Number(v) >= 60) return '#FFB547'
    return '#00E5A0'
  }
  return (
    <ResponsiveContainer width="100%" height={220} style={CHART_STYLE}>
      <BarChart data={sorted} layout="vertical" margin={{ top: 0, right: 30, left: 20, bottom: 0 }}>
        <CartesianGrid horizontal={false} stroke={GRID_COLOR} />
        <XAxis type="number" domain={[0, 100]} tick={AXIS_TICK} tickLine={false} axisLine={false} unit="%" />
        <YAxis type="category" dataKey="kitchen_id" tick={{ ...AXIS_TICK, fontSize: 9 }} tickLine={false} axisLine={false} width={60} />
        <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [`${v}%`, 'Utilization']} />
        <ReferenceLine x={100} stroke="#FF3D57" strokeDasharray="4 2" />
        {sorted.map((entry, i) => (
          <Bar key={i} dataKey="utilization_pct" fill={barColor(entry.utilization_pct)} radius={[0, 3, 3, 0]} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

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
    <div className="screen-scroll" style={{ padding: '20px 24px' }}>
      <div style={{ maxWidth: 1280, margin: '0 auto' }}>
        {/* Header */}
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'space-between', marginBottom: 28, gap: 12 }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
              <h2 style={{ fontFamily: 'Inter, sans-serif', fontWeight: 800, fontSize: 26, color: '#D4E5FF' }}>Live Dashboard</h2>
              <span style={{
                display: 'flex', alignItems: 'center', gap: 5, fontSize: 10,
                fontFamily: "'JetBrains Mono', monospace", color: '#00E5A0',
                background: 'rgba(0,229,160,0.08)', border: '1px solid rgba(0,229,160,0.2)',
                padding: '3px 10px', borderRadius: 20,
              }}>
                <span className="animate-live-pulse" style={{ width: 6, height: 6, borderRadius: '50%', background: '#00E5A0', display: 'inline-block' }} />
                LIVE
              </span>
            </div>
            <p style={{ fontSize: 11, fontFamily: "'JetBrains Mono', monospace", color: '#2D4060' }}>
              Auto-refreshes every 30s · {lastUpdated ? `Last updated ${lastUpdated.toLocaleTimeString()}` : 'Connecting...'}
            </p>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#2D4060', border: '1px solid #142038', padding: '6px 12px', borderRadius: 8 }}>
              ⚡ Batch: daily · Speed: ~30s
            </span>
            <button
              onClick={refresh}
              disabled={loading}
              style={{
                display: 'flex', alignItems: 'center', gap: 6, padding: '7px 14px', borderRadius: 8,
                border: '1px solid #142038', background: 'transparent', color: '#6B82A8',
                fontSize: 12, fontFamily: 'Inter, sans-serif', cursor: 'pointer', transition: 'all 0.15s',
              }}
              onMouseEnter={(e) => { e.currentTarget.style.color = '#D4E5FF'; e.currentTarget.style.borderColor = '#6B82A8' }}
              onMouseLeave={(e) => { e.currentTarget.style.color = '#6B82A8'; e.currentTarget.style.borderColor = '#142038' }}
            >
              <RefreshCw size={13} className={loading ? 'animate-spin' : ''} />
              Refresh
            </button>
          </div>
        </div>

        {error && (
          <div style={{ marginBottom: 20, padding: 14, borderRadius: 10, border: '1px solid rgba(255,61,87,0.3)', background: 'rgba(255,61,87,0.06)', color: '#FF3D57', fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}>
            ⚠ {error} — Run the pipeline to populate data.
          </div>
        )}

        {/* KPI row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 14, marginBottom: 20 }}>
          <KpiCard label="Total Revenue" value={fmt(kpis?.revenue_cents)} sub="from all orders" color="#00C2FF" icon={TrendingUp} />
          <KpiCard label="Orders" value={kpis?.order_count?.toLocaleString()} sub="normalised" color="#7C5CFC" icon={Zap} />
          <KpiCard
            label="Avg Delivery"
            value={kpis?.avg_delivery_min ? `${kpis.avg_delivery_min}m` : null}
            sub="SLA: 45 min"
            color={kpis?.avg_delivery_min > 43 ? '#FF3D57' : '#00E5A0'}
            icon={Clock}
          />
          <KpiCard
            label="SLA Breach"
            value={kpis?.sla_breach_pct != null ? `${kpis.sla_breach_pct}%` : null}
            sub="of deliveries"
            color={kpis?.sla_breach_pct > 20 ? '#FF3D57' : '#FFB547'}
          />
        </div>

        {/* Charts grid */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(440px, 1fr))', gap: 14, marginBottom: 14 }}>
          <div className="gk-card" style={{ padding: 20 }}>
            <SectionTitle>Revenue Trend (last 14 days)</SectionTitle>
            <RevenueLine data={data?.revenue} />
          </div>
          <div className="gk-card" style={{ padding: 20 }}>
            <SectionTitle>Orders by Platform</SectionTitle>
            <PlatformDonut data={data?.platforms} />
          </div>
          <div className="gk-card" style={{ padding: 20 }}>
            <SectionTitle>Delivery Time by Zone</SectionTitle>
            <DeliveryBar data={data?.zones} />
          </div>
          <div className="gk-card" style={{ padding: 20 }}>
            <SectionTitle>Sensor Readings by Kitchen (anomalies in tooltip)</SectionTitle>
            <SensorBar data={data?.sensors} />
          </div>
          <div className="gk-card" style={{ padding: 20 }}>
            <SectionTitle>Top Customers by LTV</SectionTitle>
            <TopCustomers data={data?.customers} />
          </div>
          <div className="gk-card" style={{ padding: 20 }}>
            <SectionTitle>Kitchen Capacity Utilisation</SectionTitle>
            <KitchenCapacity data={data?.capacity} />
          </div>
        </div>

        {/* Footer */}
        <div className="section-divider" style={{ marginBottom: 14 }} />
        <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 20, paddingBottom: 20 }}>
          {['Kafka · Spark 3.5 · Airflow · Delta Lake', 'Data Vault 2.0 · SHA-256 identity resolution', 'Lambda Architecture · Batch + Speed Layer', 'Saran Teja Mallela'].map((t) => (
            <span key={t} style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#2D4060' }}>{t}</span>
          ))}
        </div>
      </div>
    </div>
  )
}
