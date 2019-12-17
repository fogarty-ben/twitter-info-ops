package com.benfogarty.mpcs53014;

import com.opencsv.bean.CsvBindByName;

public class UserBean {	
	@CsvBindByName(column = "userid")
	private String user_id;
	
	@CsvBindByName(column = "user_display_name")
	private String user_display_name;
	
	@CsvBindByName(column = "user_screen_name")
	private String user_screen_name;
	
	@CsvBindByName(column = "user_reported_location")
	private String user_reported_location;
	
	@CsvBindByName(column = "user_profile_description")
	private String user_profile_description;
	
	@CsvBindByName(column = "user_profile_url")
	private String user_profile_url;
	
	@CsvBindByName(column = "follower_count")
	private int follower_count;
	
	@CsvBindByName(column = "following_count")
	private int following_count;
	
	@CsvBindByName(column = "account_creation_date")
	private String account_creation_date;
	private int account_creation_year;
	private int account_creation_month;
	private int account_creation_day;
	
	
	@CsvBindByName(column = "account_language")
	private String raw_account_language;
	
	
	private Date parseAccountCreationDate() {
		String parsed_date[] = account_creation_date.split("-");
		int return_date[] = new int[3]; 
		short year = Short.parseShort(parsed_date[0]);
		byte month = Byte.parseByte(parsed_date[1]);
		byte day = Byte.parseByte(parsed_date[2]);
		
		return new Date(year, month, day);
	}
	
	private AccountLanguage parseAccountLanguage() {
		switch(raw_account_language) {
			case "ar": return AccountLanguage.ARABIC;
			case "bn": return AccountLanguage.BANGLA;
			case "eu": return AccountLanguage.BASQUE;
			case "en-gb": return AccountLanguage.BRITISH_ENGLISH;
			case "bg": return AccountLanguage.BULGARIAN;
			case "ca": return AccountLanguage.CATALAN;
			case "hr": return AccountLanguage.CROATIAN;
			case "cs": return AccountLanguage.CZECH;
			case "da": return AccountLanguage.DANISH;
			case "nl": return AccountLanguage.DUTCH;
			case "en": return AccountLanguage.ENGLISH;
			case "fil": return AccountLanguage.FILIPINO;
			case "fi": return AccountLanguage.FINNISH;
			case "fr": return AccountLanguage.FRENCH;
			case "gl": return AccountLanguage.GALICIAN;
			case "de": return AccountLanguage.GERMAN;
			case "el": return AccountLanguage.GREEK;
			case "gu": return AccountLanguage.GUJARATI;
			case "he": return AccountLanguage.HEBREW;
			case "hi": return AccountLanguage.HINDI;
			case "hu": return AccountLanguage.HUNGARIAN;
			case "id": return AccountLanguage.INDONESIAN;
			case "ga": return AccountLanguage.IRISH;
			case "it": return AccountLanguage.ITALIAN;
			case "ja": return AccountLanguage.JAPANESE;
			case "kn": return AccountLanguage.KANNADA;
			case "ko": return AccountLanguage.KOREAN;
			case "msa": return AccountLanguage.MALAY;
			case "mr": return AccountLanguage.MARATHI;
			case "no": return AccountLanguage.NORWEGIAN;
			case "fa": return AccountLanguage.PERSIAN;
			case "pl": return AccountLanguage.POLISH;
			case "pt": return AccountLanguage.PORTUGUESE;
			case "ro": return AccountLanguage.ROMANIAN;
			case "ru": return AccountLanguage.RUSSIAN;
			case "sr": return AccountLanguage.SERBIAN;
			case "zh-cn": return AccountLanguage.SIMPLIFIED_CHINESE;
			case "sk": return AccountLanguage.SLOVAK;
			case "es": return AccountLanguage.SPANISH;
			case "sv": return AccountLanguage.SWEDISH;
			case "ta": return AccountLanguage.TAMIL;
			case "th": return AccountLanguage.THAI;
			case "zh-tw": return AccountLanguage.TRADITIONAL_CHINESE;
			case "tr": return AccountLanguage.TURKISH;
			case "uk": return AccountLanguage.UKRAINIAN;
			case "ur": return AccountLanguage.URDU;
			case "vi": return AccountLanguage.VIETNAMESE;
		}
		return null;
		
	}
	
	public User prepareForSerialization() {
		Date creation_date = parseAccountCreationDate();
		AccountLanguage account_language = parseAccountLanguage();
		
		User user = new User(user_id, user_display_name, user_screen_name, follower_count,
					    following_count, creation_date.year, creation_date.month, creation_date.day,
					    account_language);
		
		if (user_reported_location != null) {
			user.setUser_reported_location(user_reported_location);
		}
		
		if (user_profile_description != null) {
			user.setUser_profile_description(user_profile_description);
		}
		
		if (user_profile_url != null) {
			user.setUser_profile_url(user_profile_url);
		}
		
		return user;
		
	}
	
	public String getUserID() {
		return user_id;
	}

		
}

