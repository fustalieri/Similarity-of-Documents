
import math

root_1 = "/Users/sofos/stella/spark/ergasia/3/out 2/"
total_file = "/Users/sofos/stella/spark/ergasia/3/allFilesCosineSimilarities.csv"

file_object  = open(total_file,'r')
total_file_contents = file_object.read().split('\n')
file_object.close()


# read data from total data analysis
total_file_contents_array = []  # [[file1,file2,similarity],...]
for i in range(0,len(total_file_contents)):
    tmp = total_file_contents[i].split(';')
    total_file_contents_array.append(tmp)

# read data from per folder data analysis
seperate_file_contents_array = [] # [[file1,file2,similarity],...]
files = os.listdir(root_1) 
for f in range(0,len(files)):
    file_object  = open(root_1 + files[f],'r')
    file_content = file_object.read().split('\n')
    file_object.close()
    for i in range(0,len(file_content)):
        tmp = file_content[i].split(';')
        if(len(tmp)>3):
            seperate_file_contents_array.append([tmp[3],tmp[4],tmp[2]])


# create  array with indexes of different similarities with their similarity
same = 0
dif = 0
dif_index_array = []    # [[index1, index2,similarity1,similarity2],....]
same_index_array = []    # [[index1, index2,similarity1,similarity2],....]

for i in range(0,len(total_file_contents_array)):
    f_1 = total_file_contents_array[i][0]
    f_1_similar = total_file_contents_array[i][1]
    similarity_1 = float(total_file_contents_array[i][2])
    for j in range(0,len(seperate_file_contents_array)):
        f_2 = seperate_file_contents_array[j][0]
        if (f_1 == f_2):
            f_2_similar = seperate_file_contents_array[j][1]
            similarity_2 = float(seperate_file_contents_array[j][2])
            if(f_1_similar == f_2_similar):
                same += 1
                same_index_array.append([i,j,similarity_1,similarity_2])
            else:
                dif += 1
                dif_index_array.append([i,j,similarity_1,similarity_2])

print (str(round(dif/float(dif+same)*100,2))+ "% of total files are different (absolute value: " + str(dif) + " out of " + str(same+dif) + ")")


# get distinct categories
categories = []
for i in range(0,len(total_file_contents_array)):
    cat1 = total_file_contents_array[i][0].split('/')[0]
    if cat1 not in categories and len(cat1)>1:
        categories.append(cat1)

# get differences by category
categories_dif = []
for i in range(0,len(categories)):
    categories_dif.append(0)
    for j in range(0,len(dif_index_array)):
        item = seperate_file_contents_array[dif_index_array[j][1]]
        cat = item[0].split('/')[0]
        if(cat == categories[i]):
            categories_dif[i] += 1

categories_data = []
for i in range(0,len(categories)):
    categories_data.append((categories[i],categories_dif[i],round(categories_dif[i]/float(dif)*100,2)))

print ("different file detection distribution")
for i in range(0,len(categories_data)):
    print(categories_data[i][0] + " : " + str(categories_data[i][2]) + "%")




mo_of_dif = 0
for i in range(0,len(dif_index_array)):
    mo_of_dif += dif_index_array[i][2] - dif_index_array[i][3]

mo_of_dif = mo_of_dif/len(dif_index_array)
print("avg difference of two modes:" + str(round(mo_of_dif,3)))




mo_of_dif = 0
for i in range(0,len(dif_index_array)):
    mo_of_dif += (dif_index_array[i][2] - dif_index_array[i][3])/dif_index_array[i][3]

mo_of_dif = mo_of_dif/len(dif_index_array)
print("avg % difference of two modes:" + str(round(mo_of_dif*100,2)))




mo_total = 0
m_o_sep = 0
for i in range(0,len(dif_index_array)):
    mo_total += dif_index_array[i][2]
    m_o_sep += dif_index_array[i][3]

mo_total = mo_total/len(dif_index_array)
m_o_sep = m_o_sep/len(dif_index_array)
print("avg sim sep :" + str(round(m_o_sep,3)))
print("avg sim tot :" + str(round(mo_total,3)))



mo_not_changed = 0
for i in range(0,len(same_index_array)):
    mo_not_changed += same_index_array[i][2]

mo_not_changed = mo_not_changed/len(same_index_array)
print("avg sim not changed :" + str(round(mo_not_changed,3)))




# s_total = 0
# s_sep = 0
# for i in range(0,len(dif_index_array)):
#     s_total += (mo_total-dif_index_array[i][2])*(mo_total-dif_index_array[i][2])
#     s_sep += (m_o_sep-dif_index_array[i][3])*(m_o_sep-dif_index_array[i][3])



# s_total = math.sqrt(s_total/(len(dif_index_array)-1))
# s_sep = math.sqrt(s_sep/(len(dif_index_array)-1))


# # print average and standard deviation for 2 cases
# print("avg sim sep :" + str(round(m_o_sep,3)) + "   s :" +str(round(s_sep,3)))
# print("avg sim tot :" + str(round(mo_total,3)) + "   s :" +str(round(s_total,3)))
