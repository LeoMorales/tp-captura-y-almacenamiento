import os


def main():
    print("Hello {}!".format("World"))
    print(os.listdir())
    print(os.listdir('input'))
    print(os.listdir('logs'))

if __name__ == '__main__':
    main()
