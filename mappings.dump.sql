--
-- PostgreSQL database dump
--

-- Dumped from database version 15.3 (Debian 15.3-1.pgdg110+1)
-- Dumped by pg_dump version 15.2 (Debian 15.2-1.pgdg110+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: mapping; Type: TABLE DATA; Schema: gsp; Owner: postgres
--

COPY gsp.mapping (topic, handler) FROM stdin;
peer-height	ldg.handle_peer_height
txpool	ldg.handle_txpool
peer	ldg.handle_peer
proposed-blocks	ldg.handle_proposed_block
\.


--
-- PostgreSQL database dump complete
--

