DROP TABLE IF EXISTS sensors_data;
DROP SEQUENCE IF EXISTS sensors_data_id_seq;

CREATE SEQUENCE sensors_data_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807;

CREATE TABLE sensors_data (
	id BIGINT primary key DEFAULT nextval('sensors_data_id_seq'::regclass),
	value INT NOT NULL,
    datetime TIMESTAMP NOT NULL,
    address VARCHAR(45) NOT NULL,
	type VARCHAR(45) NOT NULL CHECK (type IN ('TEMPERATURE', 'HUMIDITY', 'LUMINOSITY'))
);


