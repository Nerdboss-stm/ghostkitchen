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
              fontFamily: 'Syne, sans-serif',
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
                fontFamily: 'Syne, sans-serif',
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

        {/* Right badge */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 11,
            color: '#2D4060',
          }}
        >
          <span
            className="animate-live-pulse"
            style={{
              width: 7,
              height: 7,
              borderRadius: '50%',
              background: '#00E5A0',
              display: 'inline-block',
            }}
          />
          <span>Railway · Vercel</span>
        </div>
      </div>
    </nav>
  )
}
