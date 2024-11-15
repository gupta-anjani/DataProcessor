CREATE TABLE table_au (
    Name VARCHAR(255) NOT NULL,             -- Mandatory and part of the key
    Cust_I VARCHAR(18) NOT NULL,                -- Mandatory
    Open_Dt DATE NOT NULL,                -- Mandatory
    last_consulted_date DATE,                        -- Optional
    VAC_ID CHAR(5),                        -- Optional
    Dr_Name CHAR(255),                      -- Optional
    state CHAR(5),                                   -- Optional
    country CHAR(5),                                 -- Optional
    DOB DATE,                              -- Optional
    FLAG CHAR(1), 
    age float8 NULL,
	days_since_last_consulted int4 NULL	-- Optional
    PRIMARY KEY (Name)                      -- Primary key on Customer Name
	
);