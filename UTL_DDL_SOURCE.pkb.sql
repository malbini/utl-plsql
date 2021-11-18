CREATE OR REPLACE PACKAGE BODY UTL_DDL_SOURCE AS

  FUNCTION GET_CURRENT_SCHEMA RETURN VARCHAR2
  IS
    l_current_schema VARCHAR2(30 CHAR);
  BEGIN
    SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') AS CURRENT_SCHEMA
    INTO l_current_schema
    FROM DUAL;
    
    RETURN l_current_schema;
  END GET_CURRENT_SCHEMA;
  
  FUNCTION EXISTS_OBJECT(p_owner IN VARCHAR2, p_object_type IN VARCHAR2, p_object_name IN VARCHAR2) RETURN BOOLEAN
  IS
    l_count INTEGER;
  BEGIN
  
    SELECT COUNT(*)
    INTO l_count
    FROM ALL_OBJECTS
    WHERE OWNER = p_owner
    AND OBJECT_TYPE = p_object_type
    AND OBJECT_NAME = p_object_name;
    
    IF l_count = 1
    THEN
      RETURN TRUE;
    END IF;
    
    RETURN FALSE;
  END EXISTS_OBJECT;
  
  PROCEDURE FREETEMPORARY(p_clob IN OUT NOCOPY CLOB) 
  IS
  BEGIN
    IF p_clob IS NOT NULL
    THEN
      DBMS_LOB.FREETEMPORARY(p_clob);
      p_clob := NULL;
    END IF;
  END FREETEMPORARY;
  
  PROCEDURE VIEW_LONG_TEXT_TO_CLOB(p_source IN OUT NOCOPY CLOB, p_owner IN VARCHAR2, p_view_name IN VARCHAR2)
  IS
    l_cursor INTEGER;
    l_n INTEGER;    
    l_long_val VARCHAR2(1024);
    l_long_len INTEGER;
    l_buflen INTEGER := 1024;
    l_curpos INTEGER := 0;
  BEGIN
    l_cursor := DBMS_SQL.OPEN_CURSOR;
    
    DBMS_SQL.PARSE(l_cursor, 'SELECT TEXT FROM ALL_VIEWS WHERE OWNER = :owner AND VIEW_NAME = :view_name', DBMS_SQL.NATIVE);
    
    DBMS_SQL.BIND_VARIABLE(l_cursor, 'owner', p_owner);
    DBMS_SQL.BIND_VARIABLE(l_cursor, 'view_name', p_view_name);
    
    DBMS_SQL.DEFINE_COLUMN_LONG(l_cursor, 1);
    
    l_n := DBMS_SQL.EXECUTE(l_cursor);
    
    DBMS_LOB.CREATETEMPORARY(lob_loc => p_source, cache => TRUE, dur => DBMS_LOB.SESSION);
    
    IF DBMS_SQL.FETCH_ROWS(l_cursor) > 0
    THEN
      
      l_long_len := 0;
      
      LOOP
        DBMS_SQL.COLUMN_VALUE_LONG(l_cursor, 1, l_buflen, l_curpos, l_long_val, l_long_len);
        EXIT WHEN l_long_len = 0;
      
        DBMS_LOB.WRITE(p_source, l_long_len, l_curpos+1, l_long_val);
        l_curpos := l_curpos + l_long_len;
        
      END LOOP;
    END IF;

    DBMS_SQL.CLOSE_CURSOR(l_cursor);
    
  EXCEPTION
    WHEN OTHERS 
      THEN        
        FREETEMPORARY(p_source);
        
        IF DBMS_SQL.IS_OPEN(l_cursor) then
          DBMS_SQL.CLOSE_CURSOR(l_cursor);
        END IF;
    
        RAISE;  
  END VIEW_LONG_TEXT_TO_CLOB;
  
  PROCEDURE READ_SOURCE_TEXT(p_source IN OUT NOCOPY CLOB, p_owner IN VARCHAR2, p_object_type IN VARCHAR2, p_object_name IN VARCHAR2)
  IS
    l_count INTEGER;
  BEGIN
  
    SELECT COUNT(*)
    INTO l_count
    FROM ALL_SOURCE
    WHERE OWNER = p_owner
    AND TYPE = p_object_type
    AND NAME = p_object_name
    AND ROWNUM <= 1;
    
    IF l_count = 0
    THEN
      RETURN;
    END IF;
    
    DBMS_LOB.CREATETEMPORARY(lob_loc => p_source, cache => TRUE, dur => DBMS_LOB.SESSION);
    
    FOR l_source_rec IN
    (
      SELECT TEXT
      FROM ALL_SOURCE
      WHERE OWNER = p_owner
      AND TYPE = p_object_type
      AND NAME = p_object_name
      ORDER BY LINE
    )
    LOOP
      DBMS_LOB.WRITEAPPEND(lob_loc => p_source, 
                           amount => LENGTH(l_source_rec.TEXT), 
                           buffer => l_source_rec.TEXT);                           
    END LOOP;
  EXCEPTION
    WHEN OTHERS 
      THEN        
        FREETEMPORARY(p_source);    
        RAISE;  
  END READ_SOURCE_TEXT;
  
  PROCEDURE EXTRACT_SOURCE(p_source IN OUT NOCOPY CLOB, p_owner IN VARCHAR2, p_object_type IN VARCHAR2, p_object_name IN VARCHAR2)
  IS
  BEGIN
    IF NOT EXISTS_OBJECT(p_owner => p_owner, p_object_type => p_object_type, p_object_name => p_object_name)
    THEN
      p_source := NULL;
      RETURN;
    END IF;
    
    IF p_object_type = 'VIEW'
    THEN
      VIEW_LONG_TEXT_TO_CLOB(p_source => p_source, p_owner => p_owner, p_view_name => p_object_name);  
    ELSE
      READ_SOURCE_TEXT(p_source => p_source, p_owner => p_owner, p_object_type => p_object_type, p_object_name => p_object_name);
    END IF;
  END EXTRACT_SOURCE;
  
  FUNCTION EXTRACT_SOURCE(p_owner IN VARCHAR2, p_object_type IN VARCHAR2, p_object_name IN VARCHAR2) RETURN CLOB
  IS
    l_source CLOB;
  BEGIN    
    EXTRACT_SOURCE(p_source => l_source, p_owner => p_owner, p_object_type => p_object_type, p_object_name => p_object_name);    
    RETURN l_source;    
  END EXTRACT_SOURCE;
  
  FUNCTION EXTRACT_SOURCE(p_object_type IN VARCHAR2, p_object_name IN VARCHAR2) RETURN CLOB
  IS
  BEGIN
    RETURN EXTRACT_SOURCE(p_owner => GET_CURRENT_SCHEMA(), p_object_type => p_object_type, p_object_name => p_object_name);
  END EXTRACT_SOURCE;
  
  PROCEDURE PURIFY_SOURCE(p_source IN OUT NOCOPY CLOB, p_purified_source IN OUT NOCOPY CLOB)
  IS
    l_length INTEGER;
    l_curr_index INTEGER;    
    l_next_eol_index INTEGER;
    
    l_line VARCHAR2(32767 CHAR);
  BEGIN
    IF p_source IS NULL
    THEN
      RETURN;
    END IF;
    
    l_curr_index := 1;
    l_length := DBMS_LOB.GETLENGTH(p_source);
    
    DBMS_LOB.CREATETEMPORARY(lob_loc => p_purified_source, cache => TRUE, dur => DBMS_LOB.SESSION);
    
    LOOP
      l_next_eol_index := DBMS_LOB.INSTR(p_source, CHR(10), l_curr_index);
      EXIT WHEN l_next_eol_index = 0;
      
      l_line := DBMS_LOB.SUBSTR (p_source, l_next_eol_index - l_curr_index, l_curr_index);      
      l_curr_index := l_next_eol_index + 1;
      
      l_line := TRIM(TRIM(CHR(9) FROM TRIM(l_line)));
      
      IF l_line IS NOT NULL
      THEN        
        DBMS_LOB.WRITEAPPEND(lob_loc => p_purified_source, 
                             amount => LENGTH(l_line) + 1, 
                             buffer => l_line || CHR(10));   
      END IF;
           
    END LOOP;
    
    IF l_curr_index < l_length
    THEN
      l_line := DBMS_LOB.SUBSTR (p_source, l_length - l_curr_index + 1, l_curr_index);
      
      l_line := TRIM(l_line);
      
      IF l_line IS NOT NULL
      THEN        
        DBMS_LOB.WRITEAPPEND(lob_loc => p_purified_source, 
                             amount => LENGTH(l_line), 
                             buffer => l_line);   
      END IF;
    END IF;
  END PURIFY_SOURCE;
  
  PROCEDURE EXTRACT_PURIFIED_SOURCE(p_purified_source IN OUT NOCOPY CLOB, p_owner IN VARCHAR2, p_object_type IN VARCHAR2, p_object_name IN VARCHAR2)
  IS  
    l_source CLOB;
  BEGIN
    EXTRACT_SOURCE(p_source => l_source, p_owner => p_owner, p_object_type => p_object_type, p_object_name => p_object_name);
    PURIFY_SOURCE(p_source => l_source, p_purified_source => p_purified_source);
    
    FREETEMPORARY(l_source);
  EXCEPTION
    WHEN OTHERS
      THEN
        FREETEMPORARY(l_source);
        FREETEMPORARY(p_purified_source);        
        RAISE;
  END EXTRACT_PURIFIED_SOURCE;
  
  FUNCTION EXTRACT_PURIFIED_SOURCE(p_owner IN VARCHAR2, p_object_type IN VARCHAR2, p_object_name IN VARCHAR2) RETURN CLOB
  IS  
    l_purified_source CLOB;
  BEGIN
    EXTRACT_PURIFIED_SOURCE(p_purified_source => l_purified_source, p_owner => p_owner, p_object_type => p_object_type, p_object_name => p_object_name);
    RETURN l_purified_source;
  END EXTRACT_PURIFIED_SOURCE;
  
  FUNCTION EXTRACT_PURIFIED_SOURCE(p_object_type IN VARCHAR2, p_object_name IN VARCHAR2) RETURN CLOB
  IS    
  BEGIN
    RETURN EXTRACT_PURIFIED_SOURCE(p_owner => GET_CURRENT_SCHEMA(), p_object_type => p_object_type, p_object_name => p_object_name);
  END EXTRACT_PURIFIED_SOURCE;
  
  FUNCTION HASH_SOURCE(p_source IN OUT NOCOPY CLOB) RETURN RAW
  IS
    l_hash RAW(20);
  BEGIN    
    IF p_source IS NOT NULL
    THEN
      l_hash := SYS.DBMS_CRYPTO.HASH(src => p_source, typ => DBMS_CRYPTO.HASH_SH1);      
      FREETEMPORARY(p_source);
    ELSE
      l_hash := NULL;
    END IF;
    
    RETURN l_hash;
  EXCEPTION
    WHEN OTHERS 
      THEN        
        FREETEMPORARY(p_source);  
        RAISE;
  END HASH_SOURCE;
  
  FUNCTION HASH_SOURCE(p_owner IN VARCHAR2, p_object_type IN VARCHAR2, p_object_name IN VARCHAR2) RETURN RAW
  IS
    l_source CLOB;
    l_hash RAW(20);
  BEGIN  
    EXTRACT_SOURCE(p_source => l_source, p_owner => p_owner, p_object_type => p_object_type, p_object_name => p_object_name);
    RETURN HASH_SOURCE(p_source => l_source);
  END HASH_SOURCE;
  
  FUNCTION HASH_SOURCE(p_object_type IN VARCHAR2, p_object_name IN VARCHAR2) RETURN RAW
  IS
  BEGIN
    RETURN HASH_SOURCE(p_owner => GET_CURRENT_SCHEMA(), p_object_type => p_object_type, p_object_name => p_object_name);  
  END HASH_SOURCE;
  
  FUNCTION HASH_PURIFIED_SOURCE(p_owner IN VARCHAR2, p_object_type IN VARCHAR2, p_object_name IN VARCHAR2) RETURN RAW
  IS
    l_source CLOB;
    l_hash RAW(20);
  BEGIN  
    EXTRACT_PURIFIED_SOURCE(p_purified_source => l_source, p_owner => p_owner, p_object_type => p_object_type, p_object_name => p_object_name);
    RETURN HASH_SOURCE(p_source => l_source);
  END HASH_PURIFIED_SOURCE;
  
  FUNCTION HASH_PURIFIED_SOURCE(p_object_type IN VARCHAR2, p_object_name IN VARCHAR2) RETURN RAW IS
  BEGIN
    RETURN HASH_PURIFIED_SOURCE(p_owner => GET_CURRENT_SCHEMA(), p_object_type => p_object_type, p_object_name => p_object_name);
  END HASH_PURIFIED_SOURCE;
  
END UTL_DDL_SOURCE;
/
