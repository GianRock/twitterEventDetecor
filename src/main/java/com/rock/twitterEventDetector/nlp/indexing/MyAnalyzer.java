package com.rock.twitterEventDetector.nlp.indexing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;

public class MyAnalyzer extends Analyzer implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3517908533689543349L;

	/** Default maximum allowed token length */
	public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

	private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;
	/**
	 * An unmodifiable set containing some common English words that are usually
	 * not useful for searching.
	 */
	public static final CharArraySet STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
	public static String[] MY_STOP_SET_FLASHMOBS = { "retweet", "tweet", "twitter", "?",
		"please", "rt.", "favorite", "from", "video", "(>_<)", "flash-mob",
		">_<", "flash", "mob", "flashmob", "flashmobs", "youtube", "rt",
		"via", "vía", "vìa", "à", "+", "%", "[", "]", "|", "‘", "’", "♥",
		"!!", "?!", "??", "!?", ",", "`", "``", "''", "-lrb-", "-rrb-",
		"-lsb-", "-rsb-", ".", ":", ";", "\"", "'", "?", "<", ">", "{",
		"}", "[", "]", "+", "-", "(", ")", "&", "%", "$", "@", "!", "^",
		"#", "*", "..", "...", "'ll", "'s", "'m", "a", "about", "above",
		"after", "again", "all", "am", "an", "and", "any", "are", "as",
		"at", "be", "because", "been", "before", "being", "below",
		"between", "both", "but", "by", "can", "can't", "cannot", "could",
		"couldn't", "did", "didn't", "do", "does", "doesn't", "doing",
		"don't", "down", "during", "each", "few", "for", "from", "further",
		"had", "hadn't", "has", "hasn't", "have", "haven't", "having",
		"he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
		"herself", "him", "himself", "his", "how", "how's", "i", "i'd",
		"i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it",
		"it's", "its", "itself", "let's", "me", "more", "most", "my",
		"myself", "no", "nor", "not", "of", "off", "on", "once", "only",
		"or", "other", "ought", "our", "ours ", "ourselves", "out", "over",
		"own", "same", "shan't", "she", "she'd", "she'll", "she's",
		"should", "shouldn't", "so", "some", "such", "than", "that",
		"that's", "the", "their", "theirs", "them", "themselves", "then",
		"there", "there's", "these", "they", "they'd", "they'll",
		"they're", "they've", "this", "those", "through", "to", "too",
		"under", "until", "up", "very", "was", "wasn't", "we", "we'd",
		"we'll", "we're", "we've", "were", "weren't", "what", "what's",
		"when", "when's", "where", "where's", "which", "while", "who",
		"who's", "whom", "why", "why's", "with", "won't", "would",
		"wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
		"yours", "yourself", "yourselves", "###", "return", "arent",
		"cant", "couldnt", "didnt", "doesnt", "dont", "hadnt", "hasnt",
		"havent", "hes", "heres", "hows", "im", "isnt", "its", "lets",
		"mustnt", "shant", "shes", "shouldnt", "thats", "theres", "theyll",
		"theyre", "theyve", "wasnt", "were", "werent", "whats", "whens",
		"wheres", "whos", "whys", "wont", "wouldnt", "youd", "youll",
		"youre", "youve" };
	public static String[] MY_STOP_SET = { "lol","flw","follow","retweet", "tweet", "twitter", "?","rt",
			"please", "rt.", "favorite", "from", "video", "(>_<)", "flash-mob",
			">_<",  "youtube", "rt",
			"via", "vía", "vìa", "à", "+", "%", "[", "]", "|", "‘", "’", "♥",
			"!!", "?!", "??", "!?", ",", "`", "``", "''", "-lrb-", "-rrb-",
			"-lsb-", "-rsb-", ".", ":", ";", "\"", "'", "?", "<", ">", "{",
			"}", "[", "]", "+", "-", "(", ")", "&", "%", "$", "@", "!", "^",
			"#", "*", "..", "...", "'ll", "'s", "'m", "a", "about", "above",
			"after", "again", "all", "am", "an", "and", "any", "are", "as",
			"at", "be", "because", "been", "before", "being", "below",
			"between", "both", "but", "by", "can", "can't", "cannot", "could",
			"couldn't", "did", "didn't", "do", "does", "doesn't", "doing",
			"don't", "down", "during", "each", "few", "for", "from", "further",
			"had", "hadn't", "has", "hasn't", "have", "haven't", "having",
			"he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
			"herself", "him", "himself", "his", "how", "how's", "i", "i'd",
			"i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it",
			"it's", "its", "itself", "let's", "me", "more", "most", "my",
			"myself", "no", "nor", "not", "of", "off", "on", "once", "only",
			"or", "other", "ought", "our", "ours ", "ourselves", "out", "over",
			"own", "same", "shan't", "she", "she'd", "she'll", "she's",
			"should", "shouldn't", "so", "some", "such", "than", "that",
			"that's", "the", "their", "theirs", "them", "themselves", "then",
			"there", "there's", "these", "they", "they'd", "they'll",
			"they're", "they've", "this", "those", "through", "to", "too",
			"under", "until", "up", "very", "was", "wasn't", "we", "we'd",
			"we'll", "we're", "we've", "were", "weren't", "what", "what's",
			"when", "when's", "where", "where's", "which", "while", "who",
			"who's", "whom", "why", "why's", "with", "won't", "would",
			"wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
			"yours", "yourself", "yourselves", "###", "return", "arent",
			"cant", "couldnt", "didnt", "doesnt", "dont", "hadnt", "hasnt",
			"havent", "hes", "heres", "hows", "im", "isnt", "its", "lets",
			"mustnt", "shant", "shes", "shouldnt", "thats", "theres", "theyll",
			"theyre", "theyve", "wasnt", "were", "werent", "whats", "whens",
			"wheres", "whos", "whys", "wont", "wouldnt", "youd", "youll",
			"youre", "youve" };

	/**
	 * Builds an analyzer with the given stop words.
	 * 
	 * @param stopWords
	 *            stop words
	 */

	@Override
	protected TokenStreamComponents createComponents(final String fieldName) {
		StandardTokenizer src = new StandardTokenizer();
		src.setMaxTokenLength(maxTokenLength);

		TokenStream tok = new WordDelimiterFilter(src, WordDelimiterFilter.GENERATE_WORD_PARTS,   null);
	 tok= new StandardFilter(tok);
		
		tok = new LowerCaseFilter(tok);
		
		
		tok = new StopFilter(tok, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
		// tok = new StopFilter(tok, StandardAnalyzer.STOP_WORDS_SET);
		tok = new StopFilter(tok, StopFilter.makeStopSet(MY_STOP_SET, true));
		tok=new LengthFilter(tok,3,this.maxTokenLength);
 		tok=new KStemFilter(tok);
		return new TokenStreamComponents(src, tok) {
			@Override
			protected void setReader(final Reader reader) throws IOException {
				 
				super.setReader(reader);
			}
		};
	}

	/**
	 * @see #setMaxTokenLength
	 */
	public int getMaxTokenLength() {
		return maxTokenLength;
	}

	/**
	 * Set maximum allowed token length. If a token is seen that exceeds this
	 * length then it is discarded. This setting only takes effect the next time
	 * tokenStream or tokenStream is called.
	 */
	public void setMaxTokenLength(int length) {
		maxTokenLength = length;
	}
public MyAnalyzer() {
	// TODO Auto-generated constructor stub
	this.maxTokenLength=50;
}


}
