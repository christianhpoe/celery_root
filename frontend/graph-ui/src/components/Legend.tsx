// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import { DEFAULT_STATE_ORDER, STATE_LABELS } from "../graph/stateColors";

export default function Legend() {
  return (
    <div className="dag-legend">
      {DEFAULT_STATE_ORDER.map((state) => (
        <div key={state} className={`dag-legend-item state-${state.toLowerCase()}`}>
          <span className="dag-legend-dot" />
          <span>{STATE_LABELS[state]}</span>
        </div>
      ))}
      <div className="dag-legend-item legend-group">
        <span className="dag-legend-dot" />
        <span>Group</span>
      </div>
      <div className="dag-legend-item legend-chord">
        <span className="dag-legend-dot" />
        <span>Chord</span>
      </div>
      <div className="dag-legend-item legend-stamp">
        <span className="dag-legend-dot" />
        <span>Stamped</span>
      </div>
      <div className="dag-legend-item legend-map">
        <span className="dag-legend-dot" />
        <span>Map</span>
      </div>
      <div className="dag-legend-item legend-starmap">
        <span className="dag-legend-dot" />
        <span>Starmap</span>
      </div>
      <div className="dag-legend-item legend-chunks">
        <span className="dag-legend-dot" />
        <span>Chunks</span>
      </div>
    </div>
  );
}
