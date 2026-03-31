/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx,ts,tsx}'],
  theme: {
    extend: {
      colors: {
        bg: '#07071a',
        card: '#0d0d24',
        border: '#1e1e3f',
        cyan: '#00d4ff',
        green: '#00ff88',
        purple: '#9945ff',
        amber: '#ffaa00',
        red: '#ff4466',
        't1': '#e8e8ff',
        't2': '#8888aa',
        't3': '#4a4a6a',
      },
      fontFamily: {
        sans: ['Inter', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
    },
  },
  plugins: [],
}
