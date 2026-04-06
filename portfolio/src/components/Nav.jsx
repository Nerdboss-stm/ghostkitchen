import { NavLink } from 'react-router-dom'

const LINKS = [
  { to: '/', label: 'Pipeline' },
  { to: '/schema', label: 'Schema' },
  { to: '/dashboard', label: 'Dashboard' },
  { to: '/map', label: 'Map' },
  { to: '/lineage', label: 'Lineage' },
]

// Bespoke Ghost Kitchen mark — ghost silhouette with chef hat
function GhostKitchenMark() {
  return (
    <svg width="26" height="30" viewBox="0 0 26 30" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="Ghost Kitchen mark">
      {/* Chef hat — three soft rounded peaks */}
      <circle cx="7.5" cy="6.5" r="4" fill="#BF953F" opacity="0.28"/>
      <circle cx="13" cy="4.5" r="5" fill="#BF953F" opacity="0.45"/>
      <circle cx="18.5" cy="6.5" r="4" fill="#BF953F" opacity="0.28"/>
      {/* Hat brim */}
      <rect x="4.5" y="10" width="17" height="2.5" rx="1.25" fill="#BF953F" opacity="0.5"/>
      {/* Ghost body */}
      <path
        d="M4.5 13.5 Q4 12.5 13 12.5 Q22 12.5 21.5 13.5 L21.5 26.5 L19 24.5 L16.5 26.5 L13 24.5 L9.5 26.5 L7 24.5 L4.5 26.5 Z"
        fill="#BF953F"
        fillOpacity="0.1"
        stroke="#BF953F"
        strokeWidth="1.2"
        strokeLinejoin="round"
      />
      {/* Ghost eyes */}
      <circle cx="9.5" cy="18" r="1.6" fill="#BF953F"/>
      <circle cx="16.5" cy="18" r="1.6" fill="#BF953F"/>
    </svg>
  )
}

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
        background: 'rgba(250, 248, 244, 0.95)',
        backdropFilter: 'blur(14px)',
        WebkitBackdropFilter: 'blur(14px)',
        borderBottom: '1px solid #D9D1C4',
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
          <GhostKitchenMark />
          <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <span
              style={{
                fontFamily: "'Cormorant Garamond', Georgia, serif",
                fontWeight: 600,
                fontStyle: 'italic',
                fontSize: 17,
                color: '#1C1A16',
                letterSpacing: '0.01em',
                lineHeight: 1,
              }}
            >
              GhostKitchen
            </span>
            <span style={{
              fontFamily: 'Inter, sans-serif',
              fontSize: 9,
              color: '#A09488',
              fontWeight: 400,
              letterSpacing: '0.04em',
              textTransform: 'uppercase',
              lineHeight: 1,
            }}>
              by Saran Teja Mallela
            </span>
          </div>
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
                color: isActive ? '#BF953F' : '#6B6256',
                background: isActive ? 'rgba(191, 149, 63, 0.07)' : 'transparent',
                border: '1px solid',
                borderColor: isActive ? 'rgba(191, 149, 63, 0.2)' : 'transparent',
              })}
            >
              {label}
            </NavLink>
          ))}
        </div>

        {/* Right — live indicator + GitHub */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#A09488' }}>
            <span className="animate-live-pulse" style={{ width: 7, height: 7, borderRadius: '50%', background: '#4A7C59', display: 'inline-block' }} />
            <span>live</span>
          </div>
          <a
            href="https://github.com/Nerdboss-stm/ghostkitchen"
            target="_blank"
            rel="noopener noreferrer"
            style={{
              display: 'flex', alignItems: 'center', gap: 6,
              fontFamily: "'JetBrains Mono', monospace", fontSize: 11,
              color: '#A09488', textDecoration: 'none',
              border: '1px solid #D9D1C4', padding: '4px 10px', borderRadius: 6,
              transition: 'color 0.15s, border-color 0.15s',
            }}
            onMouseEnter={(e) => { e.currentTarget.style.color = '#1C1A16'; e.currentTarget.style.borderColor = '#C4B99A' }}
            onMouseLeave={(e) => { e.currentTarget.style.color = '#A09488'; e.currentTarget.style.borderColor = '#D9D1C4' }}
          >
            <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.37 0 0 5.37 0 12c0 5.3 3.44 9.8 8.2 11.39.6.11.82-.26.82-.58v-2.03c-3.34.73-4.04-1.61-4.04-1.61-.54-1.38-1.33-1.75-1.33-1.75-1.09-.74.08-.73.08-.73 1.2.08 1.84 1.24 1.84 1.24 1.07 1.83 2.8 1.3 3.49 1 .11-.78.42-1.3.76-1.6-2.67-.3-5.47-1.33-5.47-5.93 0-1.31.47-2.38 1.24-3.22-.13-.3-.54-1.52.12-3.18 0 0 1.01-.32 3.3 1.23a11.5 11.5 0 0 1 3-.4c1.02 0 2.04.13 3 .4 2.28-1.55 3.29-1.23 3.29-1.23.66 1.66.25 2.88.12 3.18.77.84 1.24 1.91 1.24 3.22 0 4.61-2.81 5.63-5.48 5.92.43.37.81 1.1.81 2.22v3.29c0 .32.22.7.83.58C20.57 21.8 24 17.3 24 12c0-6.63-5.37-12-12-12z"/></svg>
            source
          </a>
        </div>
      </div>
    </nav>
  )
}
