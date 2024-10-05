 # Add a new column with the first letter of the second word from Designation_1
def get_first_letters(text):
  words = text.split()
  first_letter = words[0][0] if len(words) > 0 else ''
  second_letter = words[1][0] if len(words) > 1 else ''
  if first_letter == 'C':
        return 'Conduit'

  return first_letter + second_letter.upper()

def Type_prod(text):
    try:
        parts = text.split('/')
        parts1 = text.split()
        if len(parts) > 5:
            Type = parts[5]
            if Type == 'B':
                return 'Biaxial'
            elif Type == 'U':
                return 'Uniaxial'
            return 'Other'
        elif len(parts1) == 5:
            return 'Uniaxial'
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'



def Type_resine(text):
    try:
        parts = text.split('/')
        parts1 = text.split()
        if len(parts) > 5:
            Type = parts[4]
            if Type == 'O':
                return 'Ortho'
            elif Type == 'I':
                return 'Iso'
            elif Type == 'OI':
                return 'Ortho+Iso'
            elif Type == 'V':
                return 'Vinil'
            elif Type == 'VI':
                return 'Vinil+Iso'
            return 'Other'
        elif len(parts1) == 5:
            return 'Ortho'
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'


def DN(text):
    try:
        parts = text.split('/')
        parts1 = text.split()
        if len(parts) > 5:
          return parts[1]
        elif len(parts1) == 5:
          return parts1[1][2:]
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'
def PN(text):
    try:
        parts = text.split('/')
        parts1 = text.split()
        if len(parts) > 5:
          return parts[2]
        elif len(parts1) == 5:
          return parts1[2][2:]
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def SN(text):
    try:
        parts = text.split('/')
        parts1 = text.split()
        if len(parts) > 5:
          return parts[3] + '000'
        elif len(parts1) == 5:
          return parts1[4]
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

# ---------------------------------

def machine(text):
    try:
        # parts = text.split('')
        return text[0:2]
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def annee1(text):
    try:
        # parts = text.split('')
        return text[2:4]
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def mois(text):
    try:
        # parts = text.split('')
        return text[4:6]
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def Annee2(text):
    try:
        # parts = text.split('')
        return text[6:8]
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def N_serie_pipe(text):
    try:
        # parts = text.split('')
        return text[4:9]
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'


def angle(text):
    try:
        parts = text.split('/')
        if parts[0][:5] == 'Coude':
          return parts[0][10:] + '°'
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'


def type_p(text):
    try:
        parts = text.split('/')
        if parts[0][:5] == 'Coude':
          return parts[0][:5]
        elif len(parts) > 6:
            return parts[0]
        else:
            # Handle cases where there are not enough parts
            return parts[0]
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def nozel(text):
    try:
        parts = text.split('/')
        if parts[0] == 'Té Concentrique' or parts[0] == 'Té Tangentiel':
          return parts[2]
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def annee_p1(text):
    try:
        # parts = text.split('')
        return '20' + text[2:4]
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def DN_p(text):
    try:
        parts = text.split('/')
        if len(parts) == 5:
            return parts[1]
        if len(parts) > 5:
          return parts[1]
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def PN_p(text):
    try:
        parts = text.split('/')
        if parts[0] == 'Cone Réduction' or parts[0] == 'Reduction' or parts[0] == 'Té Tangentiel' or parts[0] == 'Té Concentrique':
            return parts[3]
        else:
            # Handle cases where there are not enough parts
            return parts[2]
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def SN_p(text):
    try:
        parts = text.split('/')
        if parts[0] == 'Cone Réduction' or parts[0] == 'Reduction' or parts[0] == 'Té Tangentiel' or parts[0] == 'Té Concentrique':
            return parts[4]
        else:
            # Handle cases where there are not enough parts
            return parts[3]
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'



def DN_j(text):
    try:
        parts = text.split('')
        if parts[0] == 'Joint':
            return parts[3]
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'

def DN_r(text):
    try:
        parts = text.split('/')
        if parts[0] == 'Raccord':
            return parts[1]
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'
def PN_r(text):
    try:
        parts = text.split('/')
        if parts[0] == 'Raccord':
            return parts[2]
        else:
            # Handle cases where there are not enough parts
            return '***'
    except Exception as e:
        # Log or print the error for debugging
        print(f"Error processing text '{text}': {e}")
        return '***'