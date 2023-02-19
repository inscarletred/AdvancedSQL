CREATE OR REPLACE FUNCTION keepcoding.fnc_clean_integer (c_integer INT64) RETURNS INT64
AS ((SELECT IFNULL(c_integer, -999999)));