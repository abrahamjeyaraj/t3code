import { AuthDiagnosticsReadScope } from "@t3tools/contracts";

import { usePrimaryEnvironment } from "../state/environments";
import { useEnvironmentQuery } from "../state/query";
import { serverEnvironment } from "../state/server";
import { useEnvironmentScope } from "../state/session";

function useMastraEnvironmentId() {
  const environmentId = usePrimaryEnvironment()?.environmentId ?? null;
  const canRead = useEnvironmentScope(environmentId, AuthDiagnosticsReadScope);
  return environmentId !== null && canRead ? environmentId : null;
}

/** Cheap probe for the sidebar: does the server have a Mastra host configured? No polling. */
export function useMastraPanelEnabled(): boolean {
  const environmentId = useMastraEnvironmentId();
  const query = useEnvironmentQuery(
    environmentId === null ? null : serverEnvironment.mastraPanelStatus({ environmentId, input: {} }),
  );
  return query.data?.enabled === true;
}

/** Polls every 15s while the page using it is mounted. */
export function useMastraPanel() {
  const environmentId = useMastraEnvironmentId();
  return useEnvironmentQuery(
    environmentId === null ? null : serverEnvironment.mastraPanel({ environmentId, input: {} }),
  );
}
