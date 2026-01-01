/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        mono: ['JetBrains Mono', 'Menlo', 'Monaco', 'Consolas', 'monospace'],
      },
      colors: {
        // Command Center Dark Theme
        cc: {
          bg: '#0a0a0c',
          'bg-dark': '#050506',
          sidebar: '#0d0d10',
          border: '#1e293b',
          card: '#0f172a',
        },
        // Accent Colors
        accent: {
          emerald: '#10b981',
          blue: '#3b82f6',
          purple: '#a855f7',
          orange: '#f97316',
          yellow: '#eab308',
          red: '#ef4444',
          cyan: '#06b6d4',
        },
      },
      animation: {
        'pulse-glow': 'pulse-glow 2s ease-in-out infinite',
        'fade-in': 'fade-in 0.15s ease-out',
        'slide-in': 'slide-in 0.3s ease-out',
        'slide-up': 'scale-in 0.15s ease-out',
        'spin-slow': 'spin 3s linear infinite',
      },
      keyframes: {
        'pulse-glow': {
          '0%, 100%': {
            boxShadow: '0 0 10px #10b981',
            opacity: '1'
          },
          '50%': {
            boxShadow: '0 0 20px #10b981',
            opacity: '0.8'
          },
        },
        'fade-in': {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        'slide-in': {
          '0%': { transform: 'translateX(-10px)', opacity: '0' },
          '100%': { transform: 'translateX(0)', opacity: '1' },
        },
        // Scale-in instead of slide-up to avoid transform conflicts with centering
        'scale-in': {
          '0%': { opacity: '0', transform: 'scale(0.95)' },
          '100%': { opacity: '1', transform: 'scale(1)' },
        },
      },
      fontSize: {
        'xxs': '0.625rem', // 10px
      },
    },
  },
  plugins: [],
}
