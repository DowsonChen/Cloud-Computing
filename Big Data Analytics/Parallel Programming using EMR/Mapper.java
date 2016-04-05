import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Mapper{
	public static String fileName = System.getenv("mapreduce_map_input_file");
	
	public static void main(String args[]) {
		try {
			InputStreamReader isReader = new InputStreamReader(System.in);
			BufferedReader br = new BufferedReader(isReader);
			String date = fileName.split("-")[2];
			String str = null;
			// While we have input on stdin
			while ((str = br.readLine()) != null) {
				partOfString = str.split(" ");
				if (QualifiedString(partOfString) == true) {				
					System.out.println(partOfString[1] + "\t" + partOfString[2] + "\t" + date);
				}
			}
			br.close();
		} catch (IOException io) {
			io.printStackTrace();
		}
	}
	
	// list the unqualified string
	static String[] specialPage = { "Media:", "Special:", "Talk:", "User:",
			"User_talk:", "Project:", "Project_talk:", "File:", "File_talk:",
			"MediaWiki:", "MediaWiki_talk:", "Template:", "Template_talk:",
			"Help:", "Help_talk:", "Category:", "Category_talk:", "Portal:",
			"Wikipedia:", "Wikipedia_talk:" };
	static String[] imageFile = { ".jpg", ".gif", ".png", ".JPG", ".GIF",
			".PNG", ".txt", ".ico" };
	static String[] boilerplate = { "404_error/", "Main_Page",
			"Hypertext_Transfer_Protocol", "Search" };

	static String[] partOfString;

	// Determine if violate the rule
	public static boolean QualifiedString(String[] partOfString) {
		if ((LengthValid(partOfString)) && (IsEnglish(partOfString[0]))
				&& (!IsSpecial(partOfString[1])) && (IsUpper(partOfString[1]))
				&& (!IsImage(partOfString[1])) && (!IsBoilplate(partOfString[1]))) {
			return true;
		} else {
			return false;
		}
	}
	
	// Determine if less than four elements
	public static boolean LengthValid(String[] str) {
		if (str != null && str.length == 4 && str[0].length() != 0 && str[1].length() != 0
				&& str[2].length() != 0 && str[3].length() != 0) {
			return true;
		}
		return false;
	}

	// Type starts begin en, case sensitive, withoud suffix
	public static boolean IsEnglish(String str) {
		if (str != null && str.equals("en")) {
			return true;
		} else {
			return false;
		}
	}

	// see if second part is special page
	public static boolean IsSpecial(String str) {
		int len = specialPage.length;
		for (int i = 0; i < len; i++) {
			if (str != null && str.startsWith(specialPage[i])) {
				return true;
			}
		}
		return false;
	}

	// see if second part starts with upper case
	public static boolean IsUpper(String str) {
		if (str != null && str.length() > 0 && !Character.isLowerCase(str.charAt(0))) {
			return true;
		} else {
			return false;
		}
	}

	// see if second part is image
	public static boolean IsImage(String str) {
		int len = imageFile.length;
		for (int i = 0; i < len; i++) {
			if (str != null && str.endsWith(imageFile[i])) {
				return true;
			}
		}
		return false;
	}

	// see if second part is boilplate
	public static boolean IsBoilplate(String str) {
		int len = boilerplate.length;
		for (int i = 0; i < len; i++) {
			if (str != null && str.equals(boilerplate[i])) {
				return true;
			}
		}
		return false;
	}
}
