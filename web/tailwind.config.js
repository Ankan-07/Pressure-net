/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: {
          950: "#070A0E",
          900: "#0B1016",
          850: "#10161E",
          800: "#161D27",
          700: "#1F2833",
          600: "#2A3441",
          500: "#3B4756",
          400: "#5A6675",
          300: "#8A95A4",
          200: "#B8C1CC",
          100: "#E4E8EE",
          50:  "#F2F4F8",
        },
        pitch: {
          DEFAULT: "#7CFF6B",
          dim:     "#5BD64C",
          glow:    "#A3FF96",
        },
        chalk:   "#F5F5F0",
        whistle: "#FFB627",
        booking: "#E5484D",
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        display: ['"Bebas Neue"', 'Impact', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      letterSpacing: {
        wider2: "0.15em",
        widest2: "0.22em",
      },
    },
  },
  plugins: [],
};
