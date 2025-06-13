// 📍 dashboard/react_app/src/components/EquityChart.tsx

import React from "react"
import { useQuery } from "@tanstack/react-query"
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts"
import { fetchEquityCurve } from "./api"

export default function EquityChart() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["equity_curve"],
    queryFn: fetchEquityCurve,
    refetchInterval: 60_000, // 1-minute refresh
  })

  if (isLoading) return <div className="text-muted text-sm">Loading chart...</div>
  if (error) return <div className="text-red-500">Failed to load equity curve</div>

  const chartData = data.equity_curve.map((point: any) => ({
    time: new Date(point.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    pnl: point.pnl,
  }))

  return (
    <div className="w-full h-64 bg-gray-900 rounded-xl p-4 text-white shadow-lg">
      <h2 className="text-lg font-semibold mb-2">Equity Curve</h2>
      <ResponsiveContainer width="100%" height="90%">
        <AreaChart data={chartData}>
          <defs>
            <linearGradient id="colorPnl" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#10b981" stopOpacity={0.8} />
              <stop offset="100%" stopColor="#10b981" stopOpacity={0} />
            </linearGradient>
          </defs>
          <XAxis dataKey="time" tick={{ fill: "#ccc", fontSize: 12 }} />
          <YAxis tick={{ fill: "#ccc", fontSize: 12 }} />
          <Tooltip />
          <Area type="monotone" dataKey="pnl" stroke="#10b981" fill="url(#colorPnl)" strokeWidth={2} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}
