
--
-- Name: yearly_hs2; Type: TABLE; Schema: raw_comtrade;
--

CREATE TABLE raw_comtrade.yearly_hs2 (
    classification text,
    year bigint,
    period bigint,
    period_desc text,
    aggregate_level bigint,
    is_leaf_code bigint,
    trade_flow_code bigint,
    trade_flow text,
    reporter_code bigint,
    reporter text,
    reporter_iso text,
    partner_code bigint,
    partner text,
    partner_iso text,
    partner_2_code text,
    partner_2 text,
    partner_2_iso text,
    customs_proc_code text,
    customs text,
    mode_of_transport_code text,
    mode_of_transport text,
    commodity_code text,
    commodity text,
    qty_unit_code bigint,
    qty_unit text,
    qty text,
    alt_qty_unit_code text,
    alt_qty_unit bigint,
    alt_qty text,
    netweight double precision,
    grossweight text,
    tradevalue bigint,
    cifvalue text,
    fobvalue text,
    flag bigint,
    UNIQUE (period, trade_flow_code, reporter_code, partner_code, 
            commodity_code, qty_unit_code, flag)
);

