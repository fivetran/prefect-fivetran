GET_CONNECTION_MOCK_RESPONSE = {
    "data": {
        "id": "12345",
        "group_id": "group_id",
        "service": "adwords",
        "service_version": 4,
        "schema": "adwords.schema",
        "paused": False,
        "pause_after_trial": True,
        "connected_by": "monitoring_assuring",
        "created_at": "2020-03-11T15:03:55.743708Z",
        "succeeded_at": "2020-03-17T12:31:40.870504Z",
        "failed_at": "2021-01-15T10:55:00.056497Z",
        "sync_frequency": 360,
        "schedule_type": "auto",
        "status": {
            "setup_state": "connected",
            "sync_state": "scheduled",
            "update_state": "delayed",
            "is_historical_sync": False,
            "tasks": [{"code": "reconnect", "message": "Reconnect"}],
            "warnings": [],
        },
        "config": {
            "sync_mode": "SpecificAccounts",
            "customer_id": "XXX-XXX-XXXX",
            "accounts": ["1234567890"],
            "conversion_window_size": 30,
            "report_type": "AD_PERFORMANCE_REPORT",
            "fields": ["PolicySummary", "AdType", "Date"],
        },
        "source_sync_details": {"accounts": ["1234567890"]},
    }
}

UPDATE_CONNECTION_MOCK_RESPONSE = {
    "code": "Success",
    "message": "Connector has been updated",
    "data": {
        "id": "speak_inexpensive",
        "group_id": "projected_sickle",
        "service": "criteo",
        "service_version": 0,
        "schema": "criteo",
        "paused": True,
        "pause_after_trial": True,
        "connected_by": "interment_burdensome",
        "created_at": "2018-12-01T15:43:29.013729Z",
        "succeeded_at": None,
        "failed_at": None,
        "sync_frequency": 60,
        "status": {
            "setup_state": "incomplete",
            "sync_state": "scheduled",
            "update_state": "on_schedule",
            "is_historical_sync": True,
            "tasks": [],
            "warnings": [],
        },
        "setup_tests": [
            {
                "title": "Validate Login",
                "status": "FAILED",
                "message": "Invalid login credentials",
            }
        ],
        "config": {
            "username": "newuser",
            "password": "******",
            "api_token": "******",
            "service_version": "0",
        },
    },
}
