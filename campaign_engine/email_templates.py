from datetime import datetime

TEMPLATES = {

    "success": {
        "subject": "Job Success",
        "body": """
        <h3>Dear Analyst,</h3>
        <p>Job completed successfully.</p>
        <p>Analyst: {analyst}</p>
        <p>Time: {time}</p>
        """
    },

    "failure": {
        "subject": "Job Failed",
        "body": """
        <h3>Dear Analyst,</h3>
        <p style='color:red;'>Job failed.</p>
        <p>Error: {error}</p>
        <p>Time: {time}</p>
        """
    }
}
