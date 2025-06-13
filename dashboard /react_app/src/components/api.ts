// 📍 dashboard/react_app/src/components/api.ts

import axios from "axios"

const BASE_URL = import.meta.env.VITE_API_URL || "https://your-backend-api.com" // <- Set this in Vercel .env

export const fetchLivePnL = async () => {
  const res = await axios.get(`${BASE_URL}/api/live_pnl`)
  return res.data
}

export const fetchEquityCurve = async () => {
  const res = await axios.get(`${BASE_URL}/api/equity_curve`)
  return res.data.equity_curve
}

export const fetchStreakStats = async (symbol: string) => {
  const res = await axios.get(`${BASE_URL}/stats/${symbol}`)
  return res.data
}
