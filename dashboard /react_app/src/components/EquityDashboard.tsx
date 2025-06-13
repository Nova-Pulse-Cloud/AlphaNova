import React, { useEffect, useState } from 'react'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'
import axios from 'axios'
import LiveStats from './LiveStats'

interface EquityPoint {
  timestamp: string
  pnl: number
}

export default function EquityDashboard() {
  const [data, setData] = useState<EquityPoint[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchEquityCurve = async () => {
      try {
        const response = await axios.get('/api/equity_curve')
        const curve = response.data.equity_curve.map((point: any) => ({
          ...point,
          timestamp: new Date(point.timestamp * 1000).toLocaleTimeString(),
        }))
        setData(curve)
      } catch (err) {
        setError('Failed to load equity data')
      } finally {
        setLoading(false)
      }
    }

    fetchEquityCurve()
  }, [])

  if (loading) return <div className="text-center text-white">Loading equity curve...</div>
  if (error) return <div className="text-center text-red-500">{error}</div>

  return (
    <div className="bg-gradient-to-b from-black via-gray-900 to-gray-950 min-h-screen p-6 text-white space-y-8">
      <h1 className="text-4xl font-extrabold text-center text-green-400">🚀 Nova Pulse Alpha Dashboard</h1>

      {/* 🔥 Live PnL Metrics Panel */}
      <LiveStats />

      {/* 📈 Equity Curve Chart */}
      <div className="w-full h-[500px] rounded-2xl border border-gray-700 shadow-lg p-4 bg-gray-900">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 20, right: 30, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis dataKey="timestamp" stroke="#ccc" fontSize={12} />
            <YAxis stroke="#ccc" fontSize={12} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1a1a1a', borderColor: '#333', color: '#fff' }}
              labelStyle={{ color: '#fff' }}
            />
            <Line type="monotone" dataKey="pnl" stroke="#4ade80" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
