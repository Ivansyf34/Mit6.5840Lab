def filter_lines(input_file, output_file):
    with open(input_file, 'r') as input_f:
        with open(output_file, 'w') as output_f:
            for line in input_f:
                #if 'applyCh' in line or 'connect' in line or '成为Leader' in line or '收到client的command' in line or '增加log' in line:
                if 'applyCh' in line :
                    output_f.write(line)

if __name__ == "__main__":
    input_file = "/home/sunyifan/study/6.5840/src/raft/20240515_125942/TestFigure8Unreliable3C_0.log"
    output_file = "/home/sunyifan/study/6.5840/src/raft/20240515_125942/TestFigure8Unreliable3C_0" + "_applych.log"
    filter_lines(input_file, output_file)
    # print("Filtered lines containing 'applyCh' or 'connect' have been saved to", output_file)
