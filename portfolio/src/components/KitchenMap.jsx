import { useEffect, useState, useCallback } from 'react'
import { MapContainer, TileLayer, CircleMarker, Popup, useMap } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { fetchKitchenCapacity } from '../lib/api'

const CITY_NAMES = {
  HOU: 'Houston', DAL: 'Dallas', AUS: 'Austin', SAT: 'San Antonio',
  FTW: 'Fort Worth', ELP: 'El Paso', ARL: 'Arlington', CRP: 'Corpus Christi',
  PLN: 'Plano', LBB: 'Lubbock',
}

const CITY_CENTERS = {
  HOU: [29.7604, -95.3698], DAL: [32.7767, -96.7970],
  AUS: [30.2672, -97.7431], SAT: [29.4241, -98.4936],
  FTW: [32.7555, -97.3308], ELP: [31.7619, -106.4850],
  ARL: [32.7357, -97.1081], CRP: [27.8006, -97.3964],
  PLN: [33.0198, -96.6989], LBB: [33.5779, -101.8552],
}

const BRAND_PATTERNS = [
  ['Burger Beast', 'Dragon Wok', 'Pizza Planet'],
  ['Taco Tornado', 'Sushi Storm', 'Burger Beast'],
  ['Pasta Palace', 'BBQ Barn', 'Dragon Wok', 'Salad Studio'],
  ['Burger Beast', 'Pizza Planet', 'Taco Tornado', 'BBQ Barn'],
  ['Dragon Wok', 'Sushi Storm', 'Pasta Palace', 'Salad Studio', 'Burger Beast'],
]

function buildKitchens() {
  const kitchens = []
  Object.entries(CITY_CENTERS).forEach(([abbrev, [lat, lon]]) => {
    for (let i = 0; i < 5; i++) {
      const angle = (i / 5) * Math.PI * 2
      const r = 0.025
      kitchens.push({
        id: `K-${abbrev}-0${i + 1}`,
        city: CITY_NAMES[abbrev],
        abbrev,
        lat: lat + Math.sin(angle) * r,
        lon: lon + Math.cos(angle) * r,
        brands: BRAND_PATTERNS[i],
        utilization: 0,
      })
    }
  })
  return kitchens
}

const BASE_KITCHENS = buildKitchens()

function markerColor(utilization, isHighlighted, isDimmed) {
  if (isDimmed) return '#C4B99A'
  if (utilization > 80) return '#C0614A'
  if (utilization > 50) return '#D4866A'
  if (utilization > 0) return '#4A7C59'
  return isHighlighted ? '#BF953F' : '#D9D1C4'
}

function MapRecenter({ center, zoom }) {
  const map = useMap()
  useEffect(() => {
    map.setView(center, zoom, { animate: true })
  }, [map, center, zoom])
  return null
}

export default function KitchenMap() {
  const [kitchens, setKitchens] = useState(BASE_KITCHENS)
  const [selectedCity, setSelectedCity] = useState(null)
  const [loading, setLoading] = useState(true)
  const [mapCenter, setMapCenter] = useState([31.0, -99.5])
  const [mapZoom, setMapZoom] = useState(6)

  useEffect(() => {
    fetchKitchenCapacity()
      .then((data) => {
        if (!data?.length) return
        setKitchens(BASE_KITCHENS.map((k) => {
          const found = data.find((d) => d.kitchen_id === k.id)
          return { ...k, utilization: found ? Number(found.utilization_pct) : 0 }
        }))
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  const handleCitySelect = useCallback((abbrev) => {
    if (selectedCity === abbrev) {
      setSelectedCity(null)
      setMapCenter([31.0, -99.5])
      setMapZoom(6)
    } else {
      setSelectedCity(abbrev)
      const [lat, lon] = CITY_CENTERS[abbrev]
      setMapCenter([lat, lon])
      setMapZoom(11)
    }
  }, [selectedCity])

  const cities = Object.keys(CITY_NAMES)
  const totalBrands = new Set(BASE_KITCHENS.flatMap((k) => k.brands)).size

  const visibleKitchens = selectedCity
    ? kitchens.filter((k) => k.abbrev === selectedCity)
    : kitchens

  const dimmedKitchens = selectedCity
    ? kitchens.filter((k) => k.abbrev !== selectedCity)
    : []

  return (
    <div className="screen" style={{ flexDirection: 'row' }}>
      {/* Sidebar */}
      <div style={{
        width: 280, flexShrink: 0, display: 'flex', flexDirection: 'column',
        borderRight: '1px solid #D9D1C4', background: '#F3EFE8',
        overflowY: 'auto',
      }}>
        {/* Sidebar header */}
        <div style={{ padding: '16px 16px 12px', borderBottom: '1px solid #D9D1C4' }}>
          <div style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: 'italic', fontWeight: 600, fontSize: 20, color: '#1C1A16', marginBottom: 3 }}>
            Kitchen Map
          </div>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488', marginBottom: 10 }}>
            Texas Ghost Kitchen Network
          </div>
          {/* Summary stats */}
          <div style={{ display: 'flex', gap: 0, borderRadius: 8, overflow: 'hidden', border: '1px solid #D9D1C4' }}>
            {[
              { v: '50', l: 'Kitchens' },
              { v: '10', l: 'Cities' },
              { v: String(totalBrands), l: 'Brands' },
            ].map(({ v, l }, i) => (
              <div key={l} style={{
                flex: 1, padding: '8px 6px', textAlign: 'center',
                background: '#EDE8DF',
                borderRight: i < 2 ? '1px solid #D9D1C4' : 'none',
              }}>
                <div style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: 'italic', fontWeight: 600, fontSize: 20, color: '#BF953F' }}>{v}</div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 9, color: '#A09488', textTransform: 'uppercase', letterSpacing: '0.06em', marginTop: 1 }}>{l}</div>
              </div>
            ))}
          </div>
        </div>

        {/* City filter */}
        <div style={{ padding: '12px 16px 0' }}>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 8 }}>
            Filter by City
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {cities.map((abbrev) => {
              const isActive = selectedCity === abbrev
              const cityKitchens = kitchens.filter((k) => k.abbrev === abbrev)
              const avgUtil = cityKitchens.reduce((s, k) => s + k.utilization, 0) / cityKitchens.length
              return (
                <button
                  key={abbrev}
                  onClick={() => handleCitySelect(abbrev)}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 10, width: '100%',
                    padding: '8px 10px', borderRadius: 8, cursor: 'pointer',
                    border: '1px solid',
                    borderColor: isActive ? 'rgba(191,149,63,0.3)' : 'transparent',
                    background: isActive ? 'rgba(191,149,63,0.07)' : 'transparent',
                    transition: 'all 0.15s', textAlign: 'left',
                  }}
                  onMouseEnter={(e) => { if (!isActive) e.currentTarget.style.background = 'rgba(28,26,22,0.03)' }}
                  onMouseLeave={(e) => { if (!isActive) e.currentTarget.style.background = 'transparent' }}
                >
                  <span style={{
                    width: 8, height: 8, borderRadius: '50%', flexShrink: 0,
                    background: avgUtil > 80 ? '#C0614A' : avgUtil > 50 ? '#D4866A' : avgUtil > 0 ? '#4A7C59' : '#D9D1C4',
                  }} />
                  <span style={{ fontFamily: 'Inter, sans-serif', fontWeight: 500, fontSize: 12, color: isActive ? '#1C1A16' : '#6B6256', flex: 1 }}>
                    {CITY_NAMES[abbrev]}
                  </span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488' }}>
                    {cityKitchens.length}
                  </span>
                  {isActive && (
                    <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 9, color: '#BF953F' }}>
                      {avgUtil > 0 ? `${Math.round(avgUtil)}%` : '–'}
                    </span>
                  )}
                </button>
              )
            })}
          </div>
        </div>

        {/* Legend */}
        <div style={{ padding: 16, marginTop: 'auto', borderTop: '1px solid #D9D1C4' }}>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 8 }}>
            Utilization
          </div>
          {[
            { color: '#4A7C59', label: '< 50% — Normal' },
            { color: '#D4866A', label: '50–80% — Busy' },
            { color: '#C0614A', label: '> 80% — At Capacity' },
            { color: '#D9D1C4', label: 'No data' },
          ].map(({ color, label }) => (
            <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 5 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: color, flexShrink: 0 }} />
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#6B6256' }}>{label}</span>
            </div>
          ))}
          {loading && (
            <div style={{ marginTop: 10, fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#A09488' }}>
              Loading utilization data...
            </div>
          )}
        </div>
      </div>

      {/* Map */}
      <div style={{ flex: 1, position: 'relative' }}>
        <MapContainer
          center={[31.0, -99.5]}
          zoom={6}
          style={{ width: '100%', height: '100%' }}
          zoomControl={true}
          attributionControl={true}
        >
          <MapRecenter center={mapCenter} zoom={mapZoom} />
          <TileLayer
            url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>'
            subdomains="abcd"
            maxZoom={19}
          />

          {/* Dimmed markers */}
          {dimmedKitchens.map((k) => (
            <CircleMarker
              key={`dim-${k.id}`}
              center={[k.lat, k.lon]}
              radius={5}
              pathOptions={{
                fillColor: '#C4B99A',
                fillOpacity: 0.35,
                color: '#D9D1C4',
                weight: 1,
              }}
            />
          ))}

          {/* Active markers */}
          {visibleKitchens.map((k) => {
            const color = markerColor(k.utilization, true, false)
            return (
              <CircleMarker
                key={k.id}
                center={[k.lat, k.lon]}
                radius={7}
                pathOptions={{
                  fillColor: color,
                  fillOpacity: 0.85,
                  color: color,
                  weight: 2,
                }}
              >
                <Popup>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", minWidth: 160 }}>
                    <div style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: 'italic', fontWeight: 600, fontSize: 15, color: '#1C1A16', marginBottom: 4 }}>
                      {k.id}
                    </div>
                    <div style={{ fontSize: 11, color: '#6B6256', marginBottom: 8 }}>{k.city}, TX</div>

                    {k.utilization > 0 && (
                      <div style={{ marginBottom: 8 }}>
                        <div style={{ fontSize: 10, color: '#A09488', marginBottom: 3 }}>UTILIZATION</div>
                        <div style={{ height: 5, borderRadius: 3, background: '#EDE8DF', marginBottom: 3 }}>
                          <div style={{ height: '100%', borderRadius: 3, width: `${k.utilization}%`, background: color }} />
                        </div>
                        <div style={{ fontSize: 12, fontWeight: 600, color }}>{k.utilization}%</div>
                      </div>
                    )}

                    <div style={{ fontSize: 10, color: '#A09488', marginBottom: 4 }}>BRANDS ({k.brands.length})</div>
                    {k.brands.map((b) => (
                      <div key={b} style={{ fontSize: 10, color: '#1C1A16', padding: '2px 0' }}>· {b}</div>
                    ))}
                  </div>
                </Popup>
              </CircleMarker>
            )
          })}
        </MapContainer>

        {/* Map overlay label */}
        <div style={{
          position: 'absolute', top: 12, right: 12, zIndex: 1000,
          background: 'rgba(250,248,244,0.92)', border: '1px solid #D9D1C4',
          borderRadius: 8, padding: '8px 12px',
          fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#6B6256',
          boxShadow: '0 2px 8px rgba(28,26,22,0.06)',
        }}>
          {selectedCity ? `${CITY_NAMES[selectedCity]} · 5 kitchens` : '50 kitchens · Texas'}
        </div>
      </div>
    </div>
  )
}
