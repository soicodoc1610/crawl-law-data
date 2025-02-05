from utils import LawVNSession, setup_logger

def main():
    print("LuatVietnam Login - First Time Setup")
    print("This will open a browser for manual login and save cookies.")
    
    session = LawVNSession(debug=True)
    if session.login():
        print("\nLogin successful and cookies saved!")
        print("You can now run 'python crawl.py' to start crawling.")
    else:
        print("\nLogin failed. Please try again.")

if __name__ == "__main__":
    main()
