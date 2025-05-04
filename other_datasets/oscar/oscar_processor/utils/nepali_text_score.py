import re
import string
from collections import Counter

class NepaliTextScorer:
    def __init__(self, config=None):
        """
        Initialize the Nepali text scorer with configuration options.
        
        Parameters:
        -----------
        config : dict, optional
            Configuration dictionary with the following possible keys:
            - use_length_score: bool, whether to score based on text length
            - use_purity_score: bool, whether to score based on language purity
            - use_formatting_score: bool, whether to score based on text formatting
            - use_content_density: bool, whether to score based on content density
            - use_readability: bool, whether to score based on readability metrics
            - weights: dict, weights for each scoring component
        """
        default_config = {
            'use_length_score': True,
            'use_purity_score': True,
            'use_formatting_score': True,
            'use_content_density': True,
            'use_readability': True,
            'weights': {
                'length': 0.15,
                'purity': 0.30,
                'formatting': 0.20,
                'content_density': 0.25,
                'readability': 0.10
            }
        }
        
        self.config = default_config if config is None else {**default_config, **config}
        
        # Nepali Unicode range (Devanagari script)
        self.nepali_char_pattern = re.compile(r'[\u0900-\u097F]')
        
        # Common Nepali punctuation and symbols
        self.nepali_punctuation = '।॥॰'
        
        # Patterns for detecting non-content elements
        self.ad_patterns = [
            r'(?i)ad[sz]?',
            r'(?i)sponsor',
            r'(?i)promo',
            r'(?i)banner',
            r'box \d+',
            r'(?i)level\d*',
            r'tata',
            r'ncell',
            r'bank',
            r'(?i)nchl',
            r'gibl'
        ]
        
    def is_nepali_char(self, char):
        """Check if a character is a Nepali character."""
        return bool(self.nepali_char_pattern.match(char)) or char in self.nepali_punctuation
    
    def calculate_length_score(self, text):
        """
        Calculate score based on text length.
        Longer texts get higher scores (with diminishing returns).
        """
        # Clean the text (remove excessive whitespace)
        clean_text = re.sub(r'\s+', ' ', text).strip()
        
        # Word count
        word_count = len(clean_text.split())
        
        # Length score with diminishing returns:
        # For very short texts (<50 words): linear growth
        # For medium texts (50-500 words): slower growth
        # For long texts (>500 words): minimal additional points
        if word_count < 50:
            return min(0.5, word_count / 100)
        elif word_count < 500:
            return 0.5 + (word_count - 50) / 900  # Grows from 0.5 to 1.0
        else:
            return min(1.0, 0.5 + 0.5 + (word_count - 500) / 2000)  # Very slow growth after 500 words
    
    def calculate_purity_score(self, text):
        """
        Calculate score based on language purity (how much of the text is in Nepali).
        """
        if not text.strip():
            return 0.0
            
        # Count Nepali and non-Nepali characters
        total_chars = 0
        nepali_chars = 0
        
        for char in text:
            if char.strip() and not char.isspace():
                total_chars += 1
                if self.is_nepali_char(char):
                    nepali_chars += 1
        
        # Prevent division by zero
        if total_chars == 0:
            return 0.0
            
        return nepali_chars / total_chars
    
    def calculate_formatting_score(self, text):
        """
        Calculate score based on text formatting quality.
        Penalizes text with ads, irrelevant content, or poor formatting.
        """
        lines = text.split('\n')
        
        # Count lines that might be ads or irrelevant content
        potential_ad_lines = 0
        for line in lines:
            line = line.strip()
            if line and any(re.search(pattern, line) for pattern in self.ad_patterns):
                potential_ad_lines += 1
        
        # Calculate ratio of clean content
        clean_ratio = 1.0
        if lines:
            non_empty_lines = sum(1 for line in lines if line.strip())
            if non_empty_lines > 0:
                clean_ratio = max(0, 1.0 - (potential_ad_lines / non_empty_lines))
        
        # Detect excessive blank lines (poor formatting)
        blank_line_ratio = 0
        if lines:
            blank_lines = sum(1 for line in lines if not line.strip())
            blank_line_ratio = blank_lines / len(lines)
        
        # Penalize too many blank lines, but don't penalize moderate spacing
        blank_line_penalty = max(0, blank_line_ratio - 0.3) * 0.5
        
        return max(0, clean_ratio - blank_line_penalty)
    
    def calculate_content_density(self, text):
        """
        Calculate the density of Nepali content relative to the total text.
        Identifies how much of the text is meaningful Nepali content vs. noise.
        """
        # Extract words
        words = re.findall(r'\b\w+\b', text)
        
        # Count words containing Nepali characters
        nepali_words = 0
        for word in words:
            if any(self.is_nepali_char(char) for char in word):
                nepali_words += 1
        
        # Prevent division by zero
        if not words:
            return 0.0
            
        return nepali_words / len(words)
    
    def calculate_readability_score(self, text):
        """
        Calculate score based on readability metrics.
        Analyzes sentence structure and complexity.
        """
        # Split text into sentences (Nepali uses "।" as sentence delimiter)
        sentences = re.split(r'[।\.\?!]', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if not sentences:
            return 0.0
        
        # Calculate average sentence length in words
        avg_sentence_length = sum(len(re.findall(r'\b\w+\b', s)) for s in sentences) / len(sentences)
        
        # Ideal sentence length is around 15-25 words
        # Score decreases for very short or very long sentences
        if avg_sentence_length < 5:
            length_score = avg_sentence_length / 5  # Penalize very short sentences
        elif avg_sentence_length <= 25:
            length_score = 1.0  # Optimal range
        else:
            length_score = max(0, 1.0 - (avg_sentence_length - 25) / 25)  # Penalize long sentences
        
        # Calculate sentence variety (standard deviation of sentence lengths)
        if len(sentences) > 1:
            lengths = [len(re.findall(r'\b\w+\b', s)) for s in sentences]
            mean_length = sum(lengths) / len(lengths)
            variance = sum((l - mean_length) ** 2 for l in lengths) / len(lengths)
            std_dev = variance ** 0.5
            
            # Some variety is good (shows varied sentence structure)
            # but too much variety might indicate inconsistent writing
            if std_dev < 2:
                variety_score = std_dev / 2  # Too uniform
            elif std_dev <= 8:
                variety_score = 1.0  # Good variety
            else:
                variety_score = max(0, 1.0 - (std_dev - 8) / 8)  # Too much variation
        else:
            variety_score = 0.5  # Only one sentence
        
        return 0.7 * length_score + 0.3 * variety_score
    
    def score_text(self, text):
        """
        Calculate the overall quality score for the given Nepali text.
        
        Parameters:
        -----------
        text : str
            The text to be scored
            
        Returns:
        --------
        dict
            A dictionary containing the overall score and individual component scores
        """
        results = {
            'overall_score': 0.0,
            'components': {}
        }
        
        # Calculate individual scores based on configuration
        scores = {}
        
        if self.config['use_length_score']:
            scores['length'] = self.calculate_length_score(text)
            results['components']['length'] = scores['length']
            
        if self.config['use_purity_score']:
            scores['purity'] = self.calculate_purity_score(text)
            results['components']['purity'] = scores['purity']
            
        if self.config['use_formatting_score']:
            scores['formatting'] = self.calculate_formatting_score(text)
            results['components']['formatting'] = scores['formatting']
            
        if self.config['use_content_density']:
            scores['content_density'] = self.calculate_content_density(text)
            results['components']['content_density'] = scores['content_density']
            
        if self.config['use_readability']:
            scores['readability'] = self.calculate_readability_score(text)
            results['components']['readability'] = scores['readability']
            
        # Calculate weighted average for overall score
        total_weight = 0
        weighted_sum = 0
        
        for component, score in scores.items():
            weight = self.config['weights'].get(component, 0)
            weighted_sum += score * weight
            total_weight += weight
            
        # Calculate overall score (normalized to 0-100 scale)
        if total_weight > 0:
            results['overall_score'] = (weighted_sum / total_weight) * 100
            
        return results

# Example usage
if __name__ == "__main__":
    # Sample texts
    text1 = """
    अझै भेटिएन हेटौंडामा सडक भासिएर बेपत्ता भएको गाडी
    सेतोपाटी संवाददाता
    सेतोपाटी संवाददाता
    काठमाडौं, वैशाख २१
    gibl

    Tata box 1
    मकवानपुरको हेटौंडामा सडक भासिएर बेपत्ता भएको गाडी अझै भेटिएको छैन।

    शुक्रबार दिउँसो १ बजे हेटौंडा उपमहानगरपालिका-१७ गैरीगाउँस्थित भित्री सडक भासिँदा त्यहाँ रोकेर राखिएको ना ४ ख ४८३३ नम्बरको गाडी नै बेपत्ता भएको थियो।

    सडक भासिएपछि त्यहाँ पानी जमेर पोखरीको रूप लिएको थियो। सडक भासिएर गाडी खस्न थालेपछि चालक हरि राई बाहिर निस्किएका थिए।

    भासिएर खसेको गाडी केही बेरमै बेपत्ता भएको थियो।

    घटनाको जानकारीपछि प्रहरी घटनास्थल पुग्दा भासिएको जमिन दलदलमा परिणत भइसकेको थियो भने गाडीको नामनिशाना देखिएको थिएन।

    जिल्ला प्रहरी कार्यालय मकवानपुरका डिएसपी श्यामु अर्यालका अनुसार शुक्रबार प्रमुख जिल्ला अधिकारीसहित प्रहरी घटनास्थल पुगेको थियो।

    Lev3Laxmi bank
    त्यहाँ विभिन्न उपाय लगाएर गाडी खोज्ने प्रयत्न गरिए पनि भेट्टाउन नसकिएको उनले बताए।

    भासिएको ठाउँमा पोखरीको स्वरूप बनेपछि दिनभरि मोटर लगाएर त्यहाँ जम्मा भएको पानी बाहिर निकालिएको थियो।

    Nchl
    Ncell
    तैपनि गाडी भेट्टाउन नसकिएको डिएसपी अर्यालले बताए।

    उनका अनुसार त्यस ठाउँमा बोरिङ खनिएको थियो। चालक र गाडी धनीसमेत रहेका हरि राईले त्यहाँ पोखरी बनाएर माछापालन समेत गरेका थिए।

    माछा बोक्न प्रयोग गर्दै आएको गाडी नै शुक्रबार बेपत्ता भएको थियो।

    आइतबार बिहानैदेखि पानी परिरहेका कारण खोजी कार्य रोकिएको उनले बताए। 'दिउँसो बैठक बसेर कसरी खोज्ने भन्ने थप उपायबारे छलफल गर्छौं,' डिएसपी अर्यालले भने।

    भासिएको सडकखण्ड नियमित सवारी साधन हिँड्ने बाटो नभएको पनि उनले बताए।  
    """

    text2 = """
    अझै भेटिएन हेटौंडामा सडक भासिएर बेपत्ता भएको गाडी

    मकवानपुरको हेटौंडामा सडक भासिएर बेपत्ता भएको गाडी अझै भेटिएको छैन।

    शुक्रबार दिउँसो १ बजे हेटौंडा उपमहानगरपालिका-१७ गैरीगाउँस्थित भित्री सडक भासिँदा त्यहाँ रोकेर राखिएको ना ४ ख ४८३३ नम्बरको गाडी नै बेपत्ता भएको थियो।

    सडक भासिएपछि त्यहाँ पानी जमेर पोखरीको रूप लिएको थियो। सडक भासिएर गाडी खस्न थालेपछि चालक हरि राई बाहिर निस्किएका थिए।

    भासिएर खसेको गाडी केही बेरमै बेपत्ता भएको थियो।

    घटनाको जानकारीपछि प्रहरी घटनास्थल पुग्दा भासिएको जमिन दलदलमा परिणत भइसकेको थियो भने गाडीको नामनिशाना देखिएको थिएन।

    जिल्ला प्रहरी कार्यालय मकवानपुरका डिएसपी श्यामु अर्यालका अनुसार शुक्रबार प्रमुख जिल्ला अधिकारीसहित प्रहरी घटनास्थल पुगेको थियो।

    त्यहाँ विभिन्न उपाय लगाएर गाडी खोज्ने प्रयत्न गरिए पनि भेट्टाउन नसकिएको उनले बताए।

    भासिएको ठाउँमा पोखरीको स्वरूप बनेपछि दिनभरि मोटर लगाएर त्यहाँ जम्मा भएको पानी बाहिर निकालिएको थियो।

    तैपनि गाडी भेट्टाउन नसकिएको डिएसपी अर्यालले बताए।

    उनका अनुसार त्यस ठाउँमा बोरिङ खनिएको थियो। चालक र गाडी धनीसमेत रहेका हरि राईले त्यहाँ पोखरी बनाएर माछापालन समेत गरेका थिए।

    माछा बोक्न प्रयोग गर्दै आएको गाडी नै शुक्रबार बेपत्ता भएको थियो।

    आइतबार बिहानैदेखि पानी परिरहेका कारण खोजी कार्य रोकिएको उनले बताए। 'दिउँसो बैठक बसेर कसरी खोज्ने भन्ने थप उपायबारे छलफल गर्छौं,' डिएसपी अर्यालले भने।

    भासिएको सडकखण्ड नियमित सवारी साधन हिँड्ने बाटो नभएको पनि उनले बताए।  
    """

    # Create scorer with default configuration
    scorer = NepaliTextScorer()
    
    # Score the texts
    result1 = scorer.score_text(text1)
    result2 = scorer.score_text(text2)
    
    print("Text 1 Score:")
    print(f"Overall Score: {result1['overall_score']:.2f}/100")
    for component, score in result1['components'].items():
        print(f"- {component}: {score:.4f}")
    
    print("\nText 2 Score:")
    print(f"Overall Score: {result2['overall_score']:.2f}/100")
    for component, score in result2['components'].items():
        print(f"- {component}: {score:.4f}")
    
    # Example of custom configuration
    custom_config = {
        'use_length_score': True,
        'use_purity_score': True,
        'use_formatting_score': True,
        'use_content_density': False,  # Disabled
        'use_readability': False,      # Disabled
        'weights': {
            'length': 0.2,
            'purity': 0.5,
            'formatting': 0.3
        }
    }
    
    print("\nWith custom configuration (only length, purity, and formatting):")
    custom_scorer = NepaliTextScorer(custom_config)
    custom_result1 = custom_scorer.score_text(text1)
    custom_result2 = custom_scorer.score_text(text2)
    
    print("Text 1 Score:")
    print(f"Overall Score: {custom_result1['overall_score']:.2f}/100")
    for component, score in custom_result1['components'].items():
        print(f"- {component}: {score:.4f}")
    
    print("\nText 2 Score:")
    print(f"Overall Score: {custom_result2['overall_score']:.2f}/100")
    for component, score in custom_result2['components'].items():
        print(f"- {component}: {score:.4f}")