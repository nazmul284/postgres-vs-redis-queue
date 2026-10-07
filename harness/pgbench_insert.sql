SET synchronous_commit = off;
INSERT INTO jobs (payload) VALUES ('{"task":"send_email","to":"user1@example.com","template":"welcome","args":{"plan":"pro","locale":"en-GB","trial_days":14,"ref":"campaign-autumn-2026"},"enqueued_at":1.0}');
