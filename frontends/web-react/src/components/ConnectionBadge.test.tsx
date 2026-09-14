import type { ConnectionState } from "@rad/core";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ConnectionBadge } from "./ConnectionBadge";

// The status wording itself (describeConnection) is tested in @rad/core.
const base: ConnectionState = { status: "open", attempt: 0, nextRetryAt: null, lastMessageAt: null };

describe("ConnectionBadge", () => {
  it("shows how old the data is while live", () => {
    const lastEventAt = new Date(Date.now() - 800).toISOString();
    render(<ConnectionBadge connection={base} lastEventAt={lastEventAt} />);
    expect(screen.getByRole("status").textContent).toMatch(/Live.*data 0\.\d s old/);
  });

  it("shows the reconnect countdown and no data age while reconnecting", () => {
    const connection: ConnectionState = {
      ...base,
      status: "reconnecting",
      attempt: 2,
      nextRetryAt: Date.now() + 2_500,
    };
    render(<ConnectionBadge connection={connection} lastEventAt={null} />);
    const text = screen.getByRole("status").textContent ?? "";
    expect(text).toMatch(/Reconnecting in [23]s \(attempt 2\)/);
    expect(text).not.toContain("old");
  });
});
