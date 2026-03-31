const BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080'

export async function triggerRun() {
  const res = await fetch(`${BASE}/run`, { method: 'POST' })
  if (!res.ok) throw new Error(`POST /run failed: ${res.status}`)
  return res.json()
}

export function streamRun(runId, onEvent, onDone) {
  const es = new EventSource(`${BASE}/run/${runId}/stream`)
  es.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data)
      if (data.stage === 'STREAM_END') {
        es.close()
        onDone?.()
      } else {
        onEvent(data)
      }
    } catch {
      // ignore parse errors
    }
  }
  es.onerror = () => {
    es.close()
    onDone?.()
  }
  return () => es.close()
}

export async function fetchKpis() {
  const res = await fetch(`${BASE}/dashboard/kpis`)
  if (!res.ok) throw new Error('KPIs failed')
  return res.json()
}

export async function fetchRevenueByDay() {
  const res = await fetch(`${BASE}/dashboard/revenue-by-day`)
  if (!res.ok) throw new Error('revenue-by-day failed')
  return res.json()
}

export async function fetchOrdersByPlatform() {
  const res = await fetch(`${BASE}/dashboard/orders-by-platform`)
  if (!res.ok) throw new Error('orders-by-platform failed')
  return res.json()
}

export async function fetchDeliveryByZone() {
  const res = await fetch(`${BASE}/dashboard/delivery-by-zone`)
  if (!res.ok) throw new Error('delivery-by-zone failed')
  return res.json()
}

export async function fetchSensorAnomalies() {
  const res = await fetch(`${BASE}/dashboard/sensor-anomalies`)
  if (!res.ok) throw new Error('sensor-anomalies failed')
  return res.json()
}

export async function fetchTopCustomers() {
  const res = await fetch(`${BASE}/dashboard/top-customers`)
  if (!res.ok) throw new Error('top-customers failed')
  return res.json()
}

export async function fetchKitchenCapacity() {
  const res = await fetch(`${BASE}/dashboard/kitchen-capacity`)
  if (!res.ok) throw new Error('kitchen-capacity failed')
  return res.json()
}

export async function fetchHealth() {
  const res = await fetch(`${BASE}/health`)
  if (!res.ok) throw new Error('health failed')
  return res.json()
}

export async function fetchAllDashboard() {
  const [kpis, revenue, platforms, zones, sensors, customers, capacity] = await Promise.all([
    fetchKpis().catch(() => null),
    fetchRevenueByDay().catch(() => []),
    fetchOrdersByPlatform().catch(() => []),
    fetchDeliveryByZone().catch(() => []),
    fetchSensorAnomalies().catch(() => []),
    fetchTopCustomers().catch(() => []),
    fetchKitchenCapacity().catch(() => []),
  ])
  return { kpis, revenue, platforms, zones, sensors, customers, capacity }
}
