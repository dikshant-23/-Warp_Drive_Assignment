
-- aap_user : Follow RLS policies, app_admin : Bypass RLS policies

CREATE ROLE app_user  WITH LOGIN PASSWORD 'appuser_secret'  NOBYPASSRLS;
CREATE ROLE app_admin WITH LOGIN PASSWORD 'appadmin_secret' BYPASSRLS;

GRANT ALL ON SCHEMA public TO app_admin;
GRANT USAGE ON SCHEMA public TO app_user;