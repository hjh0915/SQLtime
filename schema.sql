create table brch_qry_dtl (
    acc character varying(19), 
    tran_date date, 
    amt numeric(16,2), 
    dr_cr_flag integer, 
    rpt_sum character varying(8), 
    timestampl character varying(14)
);