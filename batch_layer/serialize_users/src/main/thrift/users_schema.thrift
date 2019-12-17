namespace java com.benfogarty.mpcs53014

enum AccountLanguage {
  ARABIC = 1,
  BANGLA = 2,
  BASQUE = 3,
  BRITISH_ENGLISH = 4,
  BULGARIAN = 5,
  CATALAN = 6,
  CROATIAN = 7,
  CZECH = 8,
  DANISH = 9,
  DUTCH = 10,
  ENGLISH = 11,
  FILIPINO = 12,
  FINNISH = 13,
  FRENCH = 14,
  GALICIAN = 15,
  GERMAN = 16,
  GREEK = 17,
  GUJARATI = 18,
  HEBREW = 19,
  HINDI = 20,
  HUNGARIAN = 21,
  INDONESIAN = 22,
  IRISH = 23,
  ITALIAN = 24,
  JAPANESE = 25,
  KANNADA = 26,
  KOREAN = 27,
  MALAY = 28,
  MARATHI = 29,
  NORWEGIAN = 30,
  PERSIAN = 31,
  POLISH = 32,
  PORTUGUESE = 33,
  ROMANIAN = 34,
  RUSSIAN = 35,
  SERBIAN = 36,
  SIMPLIFIED_CHINESE = 37,
  SLOVAK = 38,
  SPANISH = 39,
  SWEDISH = 40,
  TAMIL = 41,
  THAI = 42,
  TRADITIONAL_CHINESE = 43,
  TURKISH = 44,
  UKRAINIAN = 45,
  URDU = 46,
  VIETNAMESE = 47
 }
 
struct User {
  1: required string user_id;
  2: required string user_display_name;
  3: required string user_screen_name;
  4: optional string user_reported_location;
  5: optional string user_profile_description;
  6: optional string user_profile_url;
  7: required i32 follower_count;
  8: required i32 following_count;
  9: required i16 account_creation_year;
  10: required byte account_creation_month;
  11: required byte account_creation_day;
  12: required AccountLanguage account_language;
 }