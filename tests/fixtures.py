from __future__ import annotations

from io import BytesIO
import json
import zipfile


def sample_power_automate_payload() -> bytes:
    workflow = {
        "name": "Calendario Assenze Demo",
        "properties": {
            "displayName": "Calendario Assenze Demo",
            "connectionReferences": {
                "shared_sharepointonline": {"apiName": "sharepointonline"},
                "shared_approvals": {"apiName": "approvals"},
            },
            "definition": {
                "triggers": {
                    "Quando_un_elemento_viene_creato": {
                        "type": "OpenApiConnection",
                        "inputs": {
                            "host": {
                                "connection": {
                                    "name": "@parameters('$connections')['shared_sharepointonline']['connectionId']"
                                }
                            }
                        },
                    }
                },
                "actions": {
                    "CreateAnApproval": {
                        "type": "ApiConnectionWebhook",
                        "inputs": {
                            "host": {
                                "connection": {
                                    "name": "@parameters('$connections')['shared_approvals']['connectionId']"
                                }
                            },
                            "body": {
                                "title": "@{triggerBody()?['Tipoassenza']}",
                            },
                        },
                    },
                    "Until_1": {
                        "type": "Until",
                        "expression": "@equals(triggerBody()?['Datafine'],'')",
                        "actions": {
                            "Crea_elemento_2": {
                                "type": "ApiConnection",
                                "inputs": {
                                    "host": {
                                        "connection": {
                                            "name": "@parameters('$connections')['shared_sharepointonline']['connectionId']"
                                        }
                                    }
                                },
                            }
                        },
                    },
                    "Imposta_stato_di_approvazione_del_contenuto_1": {
                        "type": "ApiConnection",
                        "inputs": {
                            "host": {
                                "connection": {
                                    "name": "@parameters('$connections')['shared_sharepointonline']['connectionId']"
                                }
                            },
                            "body": {
                                "fields": [
                                    "@{triggerBody()?['Data_x0020_inizio']}",
                                    "@{triggerBody()?['Datafine']}",
                                    "@{triggerBody()?['Tipoassenza']}",
                                    "@{triggerBody()?['Motivazionerichiesta']}",
                                    "@{triggerBody()?['Salta_x0020_approvazione']}",
                                    "@{triggerBody()?['CAR']}",
                                    "@{triggerBody()?['EmailDipendente']}",
                                    "@{triggerBody()?['{ModerationStatus}']}",
                                ]
                            },
                        },
                    },
                },
            },
        },
    }

    payload = BytesIO()
    with zipfile.ZipFile(payload, "w") as zf:
        zf.writestr("workflow.json", json.dumps(workflow))
    return payload.getvalue()
