import { useState, useEffect } from 'react'
import { Zap } from 'lucide-react'

const LINKS = [
  { href: '#pipeline', label: 'Pipeline' },
  { href: '#schema', label: 'Schema' },
  { href: '#dashboard', label: 'Dashboard' },
]

export default function Nav() {
  const [active, setActive] = useState('pipeline')
  const [scrolled, setScrolled] = useState(false)

  useEffect(() => {
    const onScroll = () => {
      setScrolled(window.scrollY > 20)
      const sections = ['pipeline', 'schema', 'dashboard']
      for (const id of sections.reverse()) {
        const el = document.getElementById(id)
        if (el && window.scrollY >= el.offsetTop - 120) {
          setActive(id)
          break
        }
      }
    }
    window.addEventListener('scroll', onScroll, { passive: true })
    return () => window.removeEventListener('scroll', onScroll)
  }, [])

  return (
    <nav
      className={`fixed top-0 left-0 right-0 z-50 transition-all duration-300 ${
        scrolled ? 'bg-[#07071acc] backdrop-blur-md border-b border-[#1e1e3f]' : ''
      }`}
    >
      <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
        {/* Logo */}
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 rounded-lg bg-[#00d4ff20] border border-[#00d4ff40] flex items-center justify-center">
            <Zap size={14} className="text-cyan" />
          </div>
          <span className="font-semibold text-[#e8e8ff]">GhostKitchen</span>
          <span className="hidden sm:block text-[10px] text-[#4a4a6a] font-mono ml-1 border border-[#1e1e3f] px-2 py-0.5 rounded">
            DATA PLATFORM
          </span>
        </div>

        {/* Links */}
        <div className="flex items-center gap-1">
          {LINKS.map(({ href, label }) => {
            const id = href.replace('#', '')
            return (
              <a
                key={href}
                href={href}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
                  active === id
                    ? 'text-[#00d4ff] bg-[#00d4ff10]'
                    : 'text-[#8888aa] hover:text-[#e8e8ff] hover:bg-[#1e1e3f]'
                }`}
              >
                {label}
              </a>
            )
          })}
        </div>

        {/* Badge */}
        <div className="hidden md:flex items-center gap-2 text-xs text-[#4a4a6a] font-mono">
          <span className="w-2 h-2 rounded-full bg-[#00ff88] animate-live-pulse inline-block" />
          Railway · Vercel
        </div>
      </div>
    </nav>
  )
}
