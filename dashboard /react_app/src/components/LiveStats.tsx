import React from "react"
import { useQuery } from "@tanstack/react-query"
import { fetchLivePnL } from "./api"
import { Card, CardContent } from "@/components/ui/card"

const LiveStats = () => {
  const { data, isLoading, error } = useQuery({
    queryKey: ["live_pnl"],
    queryFn: fetchLivePnL,
    refetchInterval: 10000, // every 10 seconds
  })

  if (isLoading) return <div>Loading live PnL...</div>
  if (error) return <div>Error fetching live PnL</div>

  const { realized_pnl, unrealized_pnl, open_positions } = data

  return (
    <Card className="shadow-xl p-4 rounded-2xl bg-gradient-to-br from-gray-800 to-black text-white">
      <CardContent>
        <h2 className="text-2xl font-bold mb-4">📊 Live PnL Stats</h2>
        <div className="text-lg space-y-2">
          <div>✅ Realized PnL: <span className="font-semibold text-green-400">${realized_pnl}</span></div>
          <div>📈 Unrealized PnL: <span className="font-semibold text-yellow-400">${unrealized_pnl}</span></div>
          <div>📂 Open Positions: <span className="font-semibold">{open_positions.length}</span></div>
        </div>
      </CardContent>
    </Card>
  )
}

export default LiveStats
