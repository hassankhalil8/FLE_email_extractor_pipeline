# src/extractors/production_extractor.py
import re
import dns.resolver
from email_validator import validate_email, EmailNotValidError
from typing import List, Dict, Set
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class ProductionEmailExtractor:
    """
    Extract ALL emails with maximum accuracy using:
    1. Multiple regex patterns
    2. DNS/MX validation 
    3. Syntax validation
    4. Intelligent filtering
    """
    
    def __init__(self):
        self.mx_cache = {}
        
        # Multiple extraction patterns (catches 99% of emails)
        self.patterns = [
            # Standard emails
            r'\b[a-zA-Z0-9][a-zA-Z0-9._%+-]{0,63}@[a-zA-Z0-9][a-zA-Z0-9.-]{0,253}\.[a-zA-Z]{2,63}\b',
            
            # Emails with + addressing (gmail style)
            r'\b[a-zA-Z0-9._%+-]+\+[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
            
            # Obfuscated: user [at] domain [dot] com
            r'\b[a-zA-Z0-9._%+-]+\s*[\[\(]?\s*at\s*[\]\)]?\s*[a-zA-Z0-9.-]+\s*[\[\(]?\s*dot\s*[\]\)]?\s*[a-zA-Z]{2,}\b',
            
            # Obfuscated: user AT domain DOT com
            r'\b[a-zA-Z0-9._%+-]+\s+AT\s+[a-zA-Z0-9.-]+\s+DOT\s+[a-zA-Z]{2,}\b',
            
            # mailto: links
            r'mailto:\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
            
            # Emails in parentheses or brackets
            r'[\(\[]\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\s*[\)\]]',
            
            # Emails after "email:", "contact:", "e-mail:"
            r'(?:email|e-mail|contact|reach):\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        ]
        
        # Extensions that are NOT emails (images, files, etc.)
        self.forbidden_extensions = {
            'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'bmp', 'ico',
            'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',
            'zip', 'rar', 'tar', 'gz', '7z',
            'mp3', 'mp4', 'avi', 'mov', 'wmv',
            'css', 'js', 'json', 'xml',
            'scaled', 'circle', 'thumbnail', 'icon'
        }
        
        # Patterns to exclude (not real emails)
        self.exclusion_patterns = [
            r'@.*\.(png|jpg|jpeg|gif|svg|webp|pdf|css|js)$',  # File extensions
            r'^[0-9.]+@',  # Starts with numbers only (likely phone/date)
            r'@[0-9.]+$',  # Domain is only numbers
            r'@example\.(com|org|net)',  # Example domains
            r'@test\.',  # Test domains
            r'^(test|demo|sample|example)',  # Test emails
            r'@(localhost|127\.0\.0\.1)',  # Localhost
            r'\.\.[a-z]',  # Double dots (invalid)
        ]
    
    def extract_all_emails(self, markdown: str) -> List[Dict]:
        print(f"📧 Starting extraction from {len(markdown)} characters of markdown...")
        
        # Step 1: Extract all possible candidates
        candidates = self._extract_candidates(markdown)
        print(f"🔍 Found {len(candidates)} candidate emails")
        
        # Step 2: Normalize and clean
        normalized = self._normalize_emails(candidates)
        print(f"✨ Normalized to {len(normalized)} unique emails")
        
        # Step 3: Validate syntax
        syntax_valid = self._validate_syntax_batch(normalized)
        print(f"✅ {len(syntax_valid)} passed syntax validation")
        
        # Step 4: Validate DNS/MX records
        dns_valid = self._validate_dns_batch(syntax_valid)
        print(f"🌐 {len(dns_valid)} have valid MX records")
        
        # Step 5: Score and rank
        scored = self._score_emails(dns_valid, markdown)
        
        # Step 6: Filter out obvious garbage
        final = self._final_filter(scored)
        print(f"🎯 Final result: {len(final)} high-quality emails")
        
        return final
    
    def _extract_candidates(self, text: str) -> Set[str]:
        """Extract using ALL patterns"""
        candidates = set()
        
        for pattern in self.patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            
            # Handle tuple results (from capture groups)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0] if match[0] else match[1] if len(match) > 1 else ''
                
                if match:
                    candidates.add(str(match).strip())
        
        return candidates
    
    def _normalize_emails(self, candidates: Set[str]) -> Set[str]:
        """Normalize obfuscated emails and clean up"""
        normalized = set()
        
        for email in candidates:
            # Convert obfuscated formats
            cleaned = email.lower().strip()
            
            # user [at] domain [dot] com -> user@domain.com
            cleaned = re.sub(r'\s*[\[\(]?\s*at\s*[\]\)]?\s*', '@', cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r'\s*[\[\(]?\s*dot\s*[\]\)]?\s*', '.', cleaned, flags=re.IGNORECASE)
            
            # Remove surrounding quotes, parentheses, brackets
            cleaned = cleaned.strip('"\'"()[]<> ')
            
            # Remove trailing punctuation
            cleaned = cleaned.rstrip('.,;:')
            
            # Skip if empty or too short
            if not cleaned or len(cleaned) < 6:
                continue
            
            # Skip if matches exclusion patterns
            if any(re.search(pattern, cleaned, re.IGNORECASE) for pattern in self.exclusion_patterns):
                continue
            
            # Skip if domain ends with forbidden extension
            if '@' in cleaned:
                domain = cleaned.split('@')[1]
                domain_parts = domain.split('.')
                if len(domain_parts) > 0:
                    last_part = domain_parts[-1].lower()
                    if last_part in self.forbidden_extensions:
                        continue
            
            normalized.add(cleaned)
        
        return normalized
    
    def _validate_syntax_batch(self, emails: Set[str]) -> List[Dict]:
        """Validate email syntax using email-validator library"""
        valid = []
        
        for email in emails:
            try:
                # Use email-validator for RFC-compliant validation
                validated = validate_email(email, check_deliverability=False)
                
                valid.append({
                    'original': email,
                    'normalized': validated.normalized,
                    'local': validated.local_part,
                    'domain': validated.domain,
                    'is_valid': True
                })
            except EmailNotValidError:
                # Skip invalid emails silently
                continue
        
        return valid
    
    def _validate_dns_batch(self, emails: List[Dict]) -> List[Dict]:
        """Upgraded: Validate domains in parallel for speed"""
        validated_results = []
        # Use a pool of 20 threads to handle DNS lookups simultaneously
        with ThreadPoolExecutor(max_workers=20) as executor:
            future_to_email = {
                executor.submit(self._check_mx_with_fallback, e['domain']): e 
                for e in emails
            }
            for future in as_completed(future_to_email):
                email_data = future_to_email[future]
                try:
                    mx_info = future.result()
                    email_data.update(mx_info)
                    if mx_info['has_mx'] or mx_info['has_a_record']:
                        validated_results.append(email_data)
                except Exception:
                    continue
        return validated_results
    
    def _check_mx_with_fallback(self, domain: str) -> Dict:
        """
        Check MX records with A record fallback
        (Some servers accept mail via A record without MX)
        """
        # Use cache to avoid hammering DNS servers
        cache_key = domain.lower()
        if cache_key in self.mx_cache:
            return self.mx_cache[cache_key]
        
        result = {
            'has_mx': False,
            'has_a_record': False,
            'mx_priority': 999,
            'mx_count': 0
        }
        
        try:
            # Try MX records first
            mx_records = dns.resolver.resolve(domain, 'MX', lifetime=3)
            mx_list = list(mx_records)
            
            if mx_list:
                result['has_mx'] = True
                result['mx_count'] = len(mx_list)
                result['mx_priority'] = min(record.preference for record in mx_list)
        
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            # No MX records, try A record as fallback
            try:
                a_records = dns.resolver.resolve(domain, 'A', lifetime=3)
                if list(a_records):
                    result['has_a_record'] = True
            except:
                pass
        
        except Exception:
            # Timeout or other DNS error - give benefit of doubt
            result['has_mx'] = True  # Don't discard due to DNS issues
        
        self.mx_cache[cache_key] = result
        return result
    
    def _score_emails(self, emails: List[Dict], context: str) -> List[Dict]:
        """
        Score emails for quality (0-100)
        Higher score = more likely to be real person's email
        """
        context_lower = context.lower()
        
        for email_data in emails:
            score = 50  # Start neutral
            email = email_data['normalized']
            local = email_data['local']
            domain = email_data['domain']
            
            # === POSITIVE SIGNALS ===
            
            # Strong MX setup
            if email_data['has_mx'] and email_data['mx_priority'] < 20:
                score += 15
            
            # Multiple MX records (professional setup)
            if email_data['mx_count'] >= 2:
                score += 10
            
            # Looks like firstname.lastname or f.lastname
            if '.' in local and not local.startswith('.') and not local.endswith('.'):
                score += 20
            
            # Contains underscore (common in professional emails)
            if '_' in local:
                score += 5
            
            # Professional TLDs
            if domain.endswith(('.com', '.law', '.legal', '.org', '.net', '.us', '.uk')):
                score += 10
            
            # Appears near relevant keywords
            email_lower = email.lower()
            if email_lower in context_lower:
                # Get 200 chars before and after
                idx = context_lower.find(email_lower)
                window = context_lower[max(0, idx-200):min(len(context_lower), idx+200)]
                
                relevant_keywords = ['attorney', 'lawyer', 'partner', 'counsel', 'esq', 
                                    'contact', 'team', 'staff', 'about', 'reach']
                if any(kw in window for kw in relevant_keywords):
                    score += 15
            
            # === NEGATIVE SIGNALS ===
            
            # Generic/role-based (but don't discard - user wants ALL emails)
            generic_prefixes = ['info', 'contact', 'admin', 'support', 'sales', 
                               'hello', 'help', 'service', 'office']
            if any(local.startswith(prefix) for prefix in generic_prefixes):
                score -= 20  # Lower score but don't exclude
            
            # No-reply addresses
            if 'noreply' in local or 'no-reply' in local or 'donotreply' in local:
                score -= 30
            
            # Very long local part (often spam)
            if len(local) > 30:
                score -= 10
            
            # Lots of numbers (suspicious)
            num_digits = sum(c.isdigit() for c in local)
            if num_digits > len(local) * 0.5:  # More than 50% digits
                score -= 15
            
            # Only A record, no MX (less reliable)
            if not email_data['has_mx'] and email_data['has_a_record']:
                score -= 10
            
            # Cap score
            email_data['score'] = max(0, min(100, score))
            email_data['confidence'] = self._score_to_confidence(email_data['score'])
        
        return emails
    
    def _score_to_confidence(self, score: int) -> str:
        """Convert score to confidence level"""
        if score >= 70:
            return 'high'
        elif score >= 40:
            return 'medium'
        else:
            return 'low'
    
    def _final_filter(self, emails: List[Dict]) -> List[Dict]:
        """
        Final filtering - remove only obvious garbage
        Keep everything else (user wants ALL emails)
        """
        filtered = []
        
        for email_data in emails:
            # Only exclude if score is very low (likely garbage)
            if email_data['score'] < 20:
                continue
            
            # Must have some form of mail delivery
            if not email_data['has_mx'] and not email_data['has_a_record']:
                continue
            
            filtered.append(email_data)
        
        # Sort by score (highest first)
        return sorted(filtered, key=lambda x: x['score'], reverse=True)