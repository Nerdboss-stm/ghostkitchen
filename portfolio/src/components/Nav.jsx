import { NavLink } from 'react-router-dom'
import { Zap } from 'lucide-react'

const LINKS = [
  { to: '/', label: 'Pipeline' },
  { to: '/schema', label: 'Schema' },
  { to: '/dashboard', label: 'Dashboard' },
  { to: '/map', label: 'Map' },
  { to: '/lineage', label: 'Lineage' },
]

export default function Nav() {
  return (
    <nav
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        zIndex: 50,
        height: 56,
        background: 'rgba(4, 9, 18, 0.9)',
        backdropFilter: 'blur(12px)',
        WebkitBackdropFilter: 'blur(12px)',
        borderBottom: '1px solid #142038',
      }}
    >
      <div
        style={{
          maxWidth: 1280,
          margin: '0 auto',
          padding: '0 24px',
          height: '100%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
        }}
      >
        {/* Logo */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div
            style={{
              width: 28,
              height: 28,
              borderRadius: 8,
              background: 'rgba(0, 194, 255, 0.1)',
              border: '1px solid rgba(0, 194, 255, 0.3)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <Zap size={14} style={{ color: '#00C2FF' }} />
          </div>
          <span
            style={{
              fontFamily: 'Inter, sans-serif',
              fontWeight: 700,
              fontSize: 15,
              color: '#D4E5FF',
              letterSpacing: '-0.01em',
            }}
          >
            GhostKitchen
          </span>
          <span
            style={{
              display: 'none',
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: 10,
              color: '#2D4060',
              border: '1px solid #142038',
              padding: '2px 8px',
              borderRadius: 4,
              marginLeft: 4,
            }}
            className="sm:inline"
          >
            DATA PLATFORM
          </span>
        </div>

        {/* Links */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          {LINKS.map(({ to, label }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              style={({ isActive }) => ({
                padding: '6px 14px',
                borderRadius: 8,
                fontSize: 13,
                fontWeight: 500,
                fontFamily: 'Inter, sans-serif',
                textDecoration: 'none',
                transition: 'all 0.15s ease',
                color: isActive ? '#00C2FF' : '#6B82A8',
                background: isActive ? 'rgba(0, 194, 255, 0.08)' : 'transparent',
                border: '1px solid',
                borderColor: isActive ? 'rgba(0, 194, 255, 0.2)' : 'transparent',
              })}
            >
              {label}
            </NavLink>
          ))}
        </div>

        {/* Right — live indicator + GitHub */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#2D4060' }}>
            <span className="animate-live-pulse" style={{ width: 7, height: 7, borderRadius: '50%', background: '#00E5A0', display: 'inline-block' }} />
            <span>live</span>
          </div>
          <a
            href="https://github.com/Nerdboss-stm/ghostkitchen"
            target="_blank"
            rel="noopener noreferrer"
            style={{
              display: 'flex', alignItems: 'center', gap: 6,
              fontFamily: "'JetBrains Mono', monospace", fontSize: 11,
              color: '#2D4060', textDecoration: 'none',
              border: '1px solid #142038', padding: '4px 10px', borderRadius: 6,
              transition: 'color 0.15s, border-color 0.15s',
            }}
            onMouseEnter={(e) => { e.currentTarget.style.color = '#D4E5FF'; e.currentTarget.style.borderColor = '#1E3254' }}
            onMouseLeave={(e) => { e.currentTarget.style.color = '#2D4060'; e.currentTarget.style.borderColor = '#142038' }}
          >
            <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.37 0 0 5.37 0 12c0 5.3 3.44 9.8 8.2 11.39.6.11.82-.26.82-.58v-2.03c-3.34.73-4.04-1.61-4.04-1.61-.54-1.38-1.33-1.75-1.33-1.75-1.09-.74.08-.73.08-.73 1.2.08 1.84 1.24 1.84 1.24 1.07 1.83 2.8 1.3 3.49 1 .11-.78.42-1.3.76-1.6-2.67-.3-5.47-1.33-5.47-5.93 0-1.31.47-2.38 1.24-3.22-.13-.3-.54-1.52.12-3.18 0 0 1.01-.32 3.3 1.23a11.5 11.5 0 0 1 3-.4c1.02 0 2.04.13 3 .4 2.28-1.55 3.29-1.23 3.29-1.23.66 1.66.25 2.88.12 3.18.77.84 1.24 1.91 1.24 3.22 0 4.61-2.81 5.63-5.48 5.92.43.37.81 1.1.81 2.22v3.29c0 .32.22.7.83.58C20.57 21.8 24 17.3 24 12c0-6.63-5.37-12-12-12z"/></svg>
            source
          </a>
        </div>
      </div>
    </nav>
  )
}
