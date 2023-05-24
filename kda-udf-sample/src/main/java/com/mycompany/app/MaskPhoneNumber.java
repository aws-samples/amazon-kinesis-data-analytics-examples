package com.mycompany.app;

import java.util.*;
import java.lang.*;
import java.io.*;
import java.util.regex.*;
import org.apache.flink.table.functions.ScalarFunction;
 
public class MaskPhoneNumber extends ScalarFunction
{
	
	public String eval(String todo, String input) throws Exception{
        
        String retVal = null;
            
        switch(todo) {
            case "mask_phone": retVal = maskPhoneNumber(input); break;
            default: retVal = "no match found";
        }
        return retVal;
    }
	
	public static void main (String[] args) throws java.lang.Exception
	{
		System.out.println(maskPhoneNumber("+91 (333) 444-5678"));
		System.out.println(maskPhoneNumber("+1(333) 456-7890"));
 	}
 	private static String maskPhoneNumber(String inputPhoneNum){
         return inputPhoneNum.replaceAll("[()\\s]+", "-")
                 .replaceAll("\\d(?=(?:\\D*\\d){4})", "*");
    }
}