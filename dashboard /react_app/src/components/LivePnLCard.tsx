// 📍 dashboard/react_app/src/components/LivePnLCard.tsx

import React from "react"
import { useQuery } from "@tanstack/react-query"
import { fetchLivePnL } from "./api"

export default function LivePnLCard() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["live_pnl"],
    queryFn: fetchLivePnL,
    refetchInterval: 30_000, // Auto-refresh every 30s
  })

  if (isLoading) return <div className="text-sm text-muted">Loading PnL...</div>
  if (error) return <div className="text-red-500">Failed to load PnL</div>

  const { realized_pnl, unrealized_pnl, open_positions } = data

  return (
    <div className="rounded-2xl shadow-md p-4 bg-gradient-to-br from-gray-900 to-gray-800 text-white w-full max-w-md mx-auto">
      <h2 className="text-xl font-bold mb-4">Live PnL</h2>
      <div className="flex justify-between text-lg">
        <div>
          <p className="text-gray-400">Realized</p>
          <p className={`font-semibold ${realized_pnl >= 0 ? "text-green-400" : "text-red-400"}`}>
            ${realized_pnl.toFixed(2)}
          </p>
        </div>
        <div>
          <p className="text-gray-400">Unrealized</p>
          <p className={`font-semibold ${unrealized_pnl >= 0 ? "text-green-400" : "text-red-400"}`}>
            ${unrealized_pnl.toFixed(2)}
          </p>
        </div>
      </div>

      <div className="mt-4">
        <p className="text-gray-400 mb-2">Open Positions</p>
        <ul className="space-y-1">
          {open_positions.map((pos: any, i: number) => (
            <li key={i} className="flex justify-between text-sm">
              <span>{pos.symbol}</span>
              <span
                className={`${
                  pos.unrealized_pnl >= 0 ? "text-green-300" : "text-red-300"
                }`}
              >
                ${pos.unrealized_pnl.toFixed(2)}
              </span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}
