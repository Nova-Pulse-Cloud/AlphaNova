// dashboard/react_app/src/App.tsx

import React from "react";
import { Toaster } from "@/components/ui/sonner";

export default function App() {
  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold">Nova Pulse Dashboard 🚀</h1>
      <Toaster />
    </div>
  );
}
