 
service ProjectOne {
  string PUT(1:string key, 2:string key_type, 3:string value, 4:string value_type),
  string GET(1:string key, 2:string key_type),
  string DELETE(1:string key, 2:string key_type),
  string PUT_LOCAL(1:string key, 2:string key_type, 3:string value, 4:string value_type),
  string DELETE_LOCAL(1:string key, 2:string key_type)
}

